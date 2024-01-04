"""
File to preprocess the data from Zaptec
"""

import json
import numpy as np
import pandas as pd
import re
from datetime import timedelta, date


def make_complete_data(df_charge_session, df_nordpool, first_date=None, last_date=None, limit=10, departure24=False):
    """
    Function to make the dataset complete. Process the data from Zaptec and add energy cost
    :param df_charge_session: Dataframe of chargehistory from Zaptec
    :param df_nordpool: Dataframe of spot prices from Nordpool
    :param limit: The minimum number of kWh in order to keep the charging cycle
    :param departure24: If True the cars will depart within 24 hours
    :return: df_overview, df_detailed - overordnet og detaljert data over parkerte elbiler
    """

    # Process the data of Chargehistory from Zaptec-API
    df_charge_session, df_detailed = process_chargehistory(df_charge_session, first_date, last_date, limit, departure24)

    # Add energy cost
    df_overview, df_detailed = add_energy_cost(df_charge_session, df_detailed, df_nordpool)

    return df_overview, df_detailed


def process_chargehistory(df_charge_session, first_date=None, last_date=None, limit=10, departure24=False):
    """
    Function to process the data of Chargehistory from Zaptec-API
    :param df_charge_session: Dataframe of chargehistory from Zaptec
    :param limit: The minimum number of kWh in order to keep the charging cycle
    :param departure24: If True the cars will depart within 24 hours
    :return: dataframe of processed Charge-history data
    """
    # Change columns to datetime
    df_charge_session[['StartDateTime', 'EndDateTime', 'CommitEndDateTime']] = df_charge_session[
        ['StartDateTime', 'EndDateTime', 'CommitEndDateTime']].apply(pd.to_datetime)

    # Remove columns where EndDateTime and CommitEndDateTime is not equal
    df_charge_session = df_charge_session[df_charge_session['EndDateTime'] == df_charge_session['CommitEndDateTime']]

    # Remove rows before the start date 'first_date'
    if first_date is not None:
        first_date = pd.to_datetime(first_date)  # Konverter 'first_date' til datetime med pandas
        # Filtrer DataFrame for å beholde rader hvor 'StartDateTime' er på eller etter 'first_date'
        df_charge_session = df_charge_session[df_charge_session['StartDateTime'] >= first_date]

    # Remove rows after the end date 'last_date'
    if last_date is not None:
        last_date = pd.to_datetime(last_date)
        df_charge_session = df_charge_session[df_charge_session['EndDateTime'] < last_date]

    try:
        df_charge_session = df_charge_session.drop(columns=['Unnamed: 0'])
    except KeyError:
        pass

    # Change DeviceName to be on same format as in chargers-csv
    df_charge_session = df_charge_session.replace({'DeviceName': {'-': '', 'P': 'p', 'R': 'r', ' ': '_'}}, regex=True)

    # Delete columns where Energy is below limit
    df_charge_session = df_charge_session[df_charge_session['Energy'] > limit]

    # ======================================================================
    # Remove rows with invalid charging (too much energy compared to time)
    # ======================================================================
    time_diff = df_charge_session['EndDateTime'] - df_charge_session['StartDateTime']

    charging_power = 22.1  # Maximum charging power [kW]

    min_time = (df_charge_session['Energy'] / charging_power) * 3600  # in hours
    min_time = min_time.astype('timedelta64[s]')

    df_charge_session = df_charge_session[time_diff > min_time]  # Remove rows
    df_charge_session = df_charge_session.reset_index()

    # Process arrival time and departure time in df_overview and delete rows we don't want
    df_overview = process_arrival_departure_times(df_charge_session, departure24)

    # ==============================
    # Delete new invalid rows
    # ==============================
    # Delete cars with the same arrival and departure time (parked max 1h 57 min)
    maske = (df_overview['StartDateTime'] != df_overview['EndDateTime']) & (
                df_overview['EndDateTime'] > df_overview['StartDateTime'])
    df_overview = df_overview[maske]
    df_overview.reset_index(drop=True, inplace=True)  # reset index


    # Delete rows where the maximum charging power is not enough to charge the required amount
    time_frame = (df_overview["EndDateTime"] - df_overview["StartDateTime"])
    time_frame_hours = (time_frame.dt.total_seconds() / 3600).astype(int)

    df_overview = df_overview[time_frame_hours * charging_power > df_overview["Energy"]]
    df_overview = df_overview.reset_index(drop=True)


    df_detailed = get_detailed_charging(df_overview, departure24)

    # Delete CommitEndDateTime, TokenName and SignedSession (TokenName is only NaN, CommitEndDateTime is equal to \
    # EndDateTime and SignedSession is put into df_detailed
    df_overview = df_overview.drop(columns=['CommitEndDateTime', 'TokenName', 'SignedSession'])

    return df_overview, df_detailed


def process_arrival_departure_times(df_overview, departure24):
    """
    Function to process arrival time and departure time in df_overview to be whole hours (ex 10.00). This in order for
    the simulation to work + make sure we filter out the same data for the simulation and for the comparison
    data.
    Both the arrival- and departure time if rounded down. This is in order not to lose to many charging cycles in later
    filtering.

    :param df_overview: DataFrame of overview of charging cycles
    :return: df_overview where arrival- and departure times are whole hours.
    :param departure24: If True the cars will depart within 24 hours
    :return df_overview
    """
    # Round StartDateTime down to the closest hour (if we round up, we loose to many charging cycles)
    df_overview["StartDateTime"] = df_overview["StartDateTime"].dt.floor("H")

    # Round EndDateTime down to the beginning of the hour (in order to not charge after departure)
    df_overview["EndDateTime"] = df_overview["EndDateTime"].dt.floor("H")

    # Choose to set departure time to 24 hours
    if departure24:
        df_overview['EndDateTime'] = np.where(
            df_overview['EndDateTime'] > df_overview['StartDateTime'] + pd.Timedelta(hours=24),
            df_overview['StartDateTime'] + pd.Timedelta(hours=24),
            df_overview['EndDateTime']
        )

    # Number of hours between the departure of the last car and the arrival of car 1
    start_time = df_overview["StartDateTime"].min()  # The oldest datetime (start time for the first car)

    # Lag ankomsttimer til int, der den første timen er int 0. Timen etter dette er int 1.
    df_overview['StartHour'] = (df_overview.loc[:, 'StartDateTime'] - start_time) // pd.Timedelta('1H')
    # Gjør også dette med avreisetidspunkt ved å ta utgangspunkt i int 0 fra StartHour
    df_overview['EndHour'] = (df_overview.loc[:, 'EndDateTime'] - start_time) // pd.Timedelta('1H')

    return df_overview


def get_detailed_charging(df_charging):
    """
    Function to extract the detailed charging details for each charging sessions. There is measures for every 15 minute
    that the car is charging + start and stop time
    :param df_charging: DataFrame of the charging overview. The detailed charging details are stored in the column
                        "SignedSession"
    :return: df_detailed: DataFrame of detailed charging details.
    """
    df_detailed_charging = pd.DataFrame()

    for i in range(len(df_charging)):
        substring = df_charging["SignedSession"][i]
        substring = re.findall("RD.*]", substring)  # Extract the readings (RD)
        substring = json.loads(substring[0][4:])  # Make json-file (and remove excess symbols)

        temp_df_detailed_charging = pd.DataFrame.from_records(substring)

        # Make TM to datetime
        temp_df_detailed_charging = temp_df_detailed_charging.replace({'TM': {'T': ' ', ',.*': ''}}, regex=True)
        # Change datatypes
        temp_df_detailed_charging["TM"] = pd.to_datetime(temp_df_detailed_charging["TM"])
        temp_df_detailed_charging["RV"] = temp_df_detailed_charging["RV"].astype('float32')

        # Change column names
        temp_df_detailed_charging = temp_df_detailed_charging.rename(columns={"TM": "DateTimeUtc"})

        # Make new column with charger_id
        temp_df_detailed_charging["charge_cycle_id"] = df_charging["Id"][i]
        temp_df_detailed_charging["DeviceName"] = df_charging["DeviceName"][i]

        # Calculate how much energy that is being charged in each time slot:
        # The current RV is subtracted from the next RV.
        temp_df_detailed_charging["Charged_energy"] = np.nan

        for j in range(len(temp_df_detailed_charging)):
            if j < len(temp_df_detailed_charging) - 1:
                temp_df_detailed_charging.loc[temp_df_detailed_charging.index[j], "Charged_energy"] = \
                temp_df_detailed_charging.loc[temp_df_detailed_charging.index[j + 1], "RV"] - \
                temp_df_detailed_charging.loc[temp_df_detailed_charging.index[j], "RV"]

        df_detailed_charging = pd.concat([df_detailed_charging, temp_df_detailed_charging])

    return df_detailed_charging


def add_energy_cost(df_overview, df_detailed, df_spot_prices):
    """
    Function to add the energy cost
    :param df_overview: DataFrame of overview of charging cycles
    :param df_detailed: DataFrame of detailed charging details.
    :param df_spot_prices: DataFrame with hourly spot prices from NO1
    :return: df_overview and df_detailed: DataFrames with new column of energy cost
    """
    df_detailed = df_detailed.reset_index()

    # ===================================================
    # Add energy cost for each timestamp in df_detailed
    # ===================================================
    for k in range(len(df_detailed)):
        # Round down to the closest hour. The spot price is pr hour.
        rounded_datetime = df_detailed.loc[df_detailed.index[k], "DateTimeUtc"].floor("H")
        # Find spot price for the given hour
        spot_price = df_spot_prices.loc[rounded_datetime, "Price [NOK/MWh]"]
        # Add the energy cost
        df_detailed.loc[df_detailed.index[k], "energy_cost"] = spot_price * df_detailed.loc[df_detailed.index[k], "Charged_energy"] / 1000

    # =============================================================
    # Add total energy cost for each charging cycle in df_overview
    # =============================================================
    for cycle_id in df_detailed["charge_cycle_id"].unique():
        total_cost = df_detailed.loc[df_detailed["charge_cycle_id"] == cycle_id, 'energy_cost'].sum()
        df_overview.loc[df_overview["Id"] == cycle_id, "total_energy_cost"] = total_cost

    return df_detailed, df_overview


def remove_etterlading(df_detailed, limit=10, departure24=False):
    """
    Function to only look at charging happening within 24 hours, and thereby excluding 'etterlading'.
    :param df_detailed: Processed df_detailed that also contains costs.
    :param limit: Minimum limit for how much energy the car needs to charge the first day in order to be included
    :param: departure24: 'True' means that the car has departe 24 timer after arrival. 'False' means the original
                          departure time is being used
    :return: df_overview_wo_etterlading - DataFrame with overview of charging for the first 24 hour of each charging cycle
    """
    # Sorter data
    df_detailed = df_detailed.sort_values(by=['charge_cycle_id', 'DateTimeUtc'])

    # Find start time for each ID
    start_times = df_detailed.groupby('charge_cycle_id')['DateTimeUtc'].first()

    def filter_rows(group):
        """Slett all lading som skjer etter 24 timer"""
        start_time = start_times[group.name]
        return group[group['DateTimeUtc'] <= start_time + timedelta(hours=24)]

    # Behold bare lading som skjer innenfor 24 timer fra første måling per bil
    df_detailed_wo_etterlading = df_detailed.groupby('charge_cycle_id').apply(filter_rows).reset_index(drop=True)

    df_grouped_first_day = df_detailed_wo_etterlading[["Charged_energy", "charge_cycle_id", "energy_cost"]].groupby(
        "charge_cycle_id").sum()

    # =================================================================
    # Lag ny df_overview: Ved hjelp av data fra df_detailed_first_day
    # =================================================================

    # Gjør start_times om til en DataFrame
    start_times_df = start_times.reset_index()

    # Merge df_grouped_first_day med start_times_df
    df_overview_wo_etterlading = pd.merge(df_grouped_first_day, start_times_df, on='charge_cycle_id', how='left')

    # Finn slutttidspunktet for hver charge_cycle_id i df_detailed_first_day
    end_times = df_detailed.groupby('charge_cycle_id')['DateTimeUtc'].last().reset_index()

    # Merge df_merged med end_times
    df_overview_wo_etterlading = pd.merge(df_overview_wo_etterlading, end_times, on='charge_cycle_id', how='left')

    # Endre navn på kolonner
    df_overview_wo_etterlading = df_overview_wo_etterlading.rename(
        columns={'DateTimeUtc_x': 'StartDateTime', 'DateTimeUtc_y': 'EndDateTime', 'Charged_energy': 'Energy'})

    # ==============================================================
    # Slett rader der ladet energi er mindre enn bestemt grenseverdi
    # ==============================================================
    df_overview_wo_etterlading = df_overview_wo_etterlading[df_overview_wo_etterlading['Energy'] > limit]

    # Finn de unike charge_cycle_id-verdiene i df_overview_wo_etterlading
    unique_ids = df_overview_wo_etterlading['charge_cycle_id'].unique()

    # Slett de samme ladesyklusene i df_detailed som du gjorde ovenfor med limit=1 for df_overview
    df_detailed_wo_etterlading = df_detailed_wo_etterlading[
        df_detailed_wo_etterlading['charge_cycle_id'].isin(unique_ids)]

    # Reset indexer
    df_overview_wo_etterlading = df_overview_wo_etterlading.reset_index(drop=True)  # Reset index
    df_detailed_wo_etterlading = df_detailed_wo_etterlading.drop('index', axis=1)   # Slett ekstra index-kolonne
    df_detailed_wo_etterlading = df_detailed_wo_etterlading.reset_index(drop=True)  # Reset index

    # =====================================================================
    # Endre slik at EndDateTime er maksimalt 30 dager etter StartDateTime
    # =====================================================================
    max_interval = timedelta(days=30)

    # Oppdater EndDateTime der forskjellen er større enn maksimalt tillatt
    df_overview_wo_etterlading['EndDateTime'] = np.where(
        (df_overview_wo_etterlading['EndDateTime'] - df_overview_wo_etterlading['StartDateTime']) > max_interval,
        df_overview_wo_etterlading['StartDateTime'] + max_interval,
        df_overview_wo_etterlading['EndDateTime'])

    df_overview_wo_etterlading = process_arrival_departure_times(df_overview_wo_etterlading, departure24)

    # ======================================================================
    # Legg til maksimal effekt
    # ======================================================================
    # Finn max charged_energy
    df_max_power = df_detailed_wo_etterlading.groupby('charge_cycle_id')['Charged_energy'].max().reset_index()

    # Beregn effekt ut ifra antakelse om at målingen er per 15 minutt (altså 0,25 time). Rund av til 1 desimal (rund alltid opp)
    df_max_power['Max_power [kW]'] = np.ceil(df_max_power['Charged_energy'] / 0.25 * 10) / 10

    # Få kolonnen 'Max_power [kW]' inn i df_overview basert på matchende 'charge_cycle_id' fra df_detailed.
    df_overview_wo_etterlading = pd.merge(df_overview_wo_etterlading,
                                          df_max_power[['charge_cycle_id', 'Max_power [kW]']], on='charge_cycle_id',
                                          how='left')

    df_overview_wo_etterlading = df_overview_wo_etterlading.reset_index()

    return df_detailed_wo_etterlading, df_overview_wo_etterlading
