import pandas as pd
from optimization_model import get_df_overview


def get_real_costs(df_overview_csv, df_detailed_csv, df_spotprice_csv, peak_cost_dict, start_time):
    """
    Funksjon for å beregne kostnadene ved reell lastprofil ved ønsket starttidspunkt.
    For å gjøre dette blir starttidspunktet i df_detailed og df_overview forandret til 'start_time'

    :param df_overview_csv: csv with overview of EV data
    :param df_detailed_csv: csv with detailed EV data
    :param df_spotprice_csv: csv with spot prices
    :param peak_cost_dict: dict av effekttariffer
    :param start_time: Ønsket starttidspunkt for simuleringen
    :return: total_monthly_costs (DataFrame med månedlige kostnader), df_overview_shifted (flyttet df_overview til ønsket starttidspunkt), df_detailed_shifted (flyttet df_detailed til ønsket starttidspunkt)
    """
    # Last inn filer
    df_detailed = pd.read_csv(df_detailed_csv)
    df_spotprice = pd.read_csv(df_spotprice_csv, index_col="DateTimeUtc")  # Load csv
    df_overview = get_df_overview(df_overview_csv)
    df_detailed["DateTimeUtc"] = pd.to_datetime(
        df_detailed["DateTimeUtc"])  # Change DateTime-columns to pandas DateTime
    df_spotprice.index = pd.to_datetime(df_spotprice.index)  # Change index to datatime dtype
    start_time = pd.to_datetime(start_time)

    # Flytt starttidspunkt til ønsket starttid for simuleringen
    df_overview_shifted, df_detailed_shifted = shift_dataframe_time(
        df_overview, df_detailed, start_time)

    # Legg til riktig energikostnad i df_overview og df_detailed
    df_detailed_shifted, df_overview_shifted = add_energy_cost(
        df_overview_shifted, df_detailed_shifted, df_spotprice)

    # Get load profile
    load_df = df_detailed_shifted.groupby(pd.Grouper(key='DateTimeUtc', freq='H'))[
        ['Charged_energy', 'energy_cost']].sum()

    # Finn månedlige totale kostnader (både effektkostnader og strømkostnader)
    total_monthly_costs = calculate_monthly_costs(df_detailed_shifted, load_df, peak_cost_dict)

    return total_monthly_costs, df_overview_shifted, df_detailed_shifted, load_df


def shift_dataframe_time(df_overview, df_detailed, time_string):
    """
    Forskyver tidspunktene i en spesifikk kolonne av en DataFrame basert på forskjellen
    mellom en gitt tid og minimumsverdien i den kolonnen.

    :param df_overview: pandas DataFrame som inneholder overordnet oversikt over ladehistorikk
    :param df_detailed: pandas DataFrame som inneholder kolonnen med tidspunktene
    (og detaljert ladehistorikk)
    :param time_string: Tidspunkt gitt som en streng. Dette er starttidspunktet for simuleringen.
    :return: En ny DataFrame med tidspunktene forskjøvet.
    """

    # Sikre at kolonnen er i datetime-format
    df_detailed["DateTimeUtc"] = pd.to_datetime(df_detailed["DateTimeUtc"])

    # Finn og rund ned minimumsverdien i angitt kolonne til nærmeste time
    min_time = df_detailed["DateTimeUtc"].min().floor('H')

    # Konverter streng til datetime
    specified_time = pd.to_datetime(time_string)

    # Beregn forskjellen i timer
    hours_to_shift = (specified_time - min_time).total_seconds() / 3600

    # Forskyv tidspunktene i df_detailed
    df_detailed_shifted = df_detailed.copy()
    df_detailed_shifted["DateTimeUtc"] = df_detailed_shifted["DateTimeUtc"] + pd.Timedelta(hours=int(hours_to_shift))

    # Forskyv tidspunktene i df_overview
    df_overview_shifted = df_overview.copy()
    df_overview_shifted[['StartDateTime', 'EndDateTime']] = df_overview_shifted[['StartDateTime', 'EndDateTime']].apply(
        lambda x: x + pd.Timedelta(hours=int(hours_to_shift)))

    return df_overview_shifted, df_detailed_shifted


def add_energy_cost(df_overview, df_detailed, df_spot_prices):
    """
    Add energy cost to df_detailed and df_overview by using prices from df_spot_prices
    :param df_overview: DataFrame with overview charging data
    :param df_detailed: DataFrame with detailed charging data
    :param df_spot_prices: DataFrame with spot prices
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

        df_detailed.loc[df_detailed.index[k], "energy_cost"] = spot_price * df_detailed.loc[
            df_detailed.index[k], "Charged_energy"] / 1000  # Add energy cost

    # =============================================================
    # Add total energy cost for each charging cycle in df_overview
    # =============================================================
    for cycle_id in df_detailed["charge_cycle_id"].unique():
        total_cost = df_detailed.loc[df_detailed["charge_cycle_id"] == cycle_id, 'energy_cost'].sum()
        df_overview.loc[df_overview["charge_cycle_id"] == cycle_id, "energy_cost"] = total_cost

    return df_detailed, df_overview


def calculate_monthly_costs(df_detailed, load_df, peak_cost_dict):
    """
    Calculate monthly costs - energy cost, power cost and total cost
    :param df_detailed: Detailed overview of charging
    :param load_df: Aggregated hourly load profile for all cars
    :param peak_cost_dict: Monthly peak tariff
    :return: DataFrame of monthly costs
    """
    # Grupper dataene etter år og måned, og finn indeksen for maksimalt forbruk i hver gruppe
    idx = load_df.groupby([load_df.index.month])['Charged_energy'].idxmax()

    # Bruk indeksene fra forrige trinn til å hente de relevante radene fra den originale DataFrame
    highest_per_month = load_df.loc[idx]

    highest_per_month["Month"] = idx.index
    highest_per_month = highest_per_month.sort_index()

    df_peak_cost = highest_per_month[["Month", "Charged_energy"]]

    df_peak_cost['Monthly_Peak_Cost'] = df_peak_cost['Month'].map(peak_cost_dict) * df_peak_cost["Charged_energy"]

    df_peak_cost = df_peak_cost.rename(columns={"Charged_energy": "Peak_load"})

    total_monthly_cost = df_detailed.groupby(pd.Grouper(key='DateTimeUtc', freq='M'))[
        ['Charged_energy', 'energy_cost']].sum()

    total_monthly_cost["Month"] = total_monthly_cost.index.month

    # Slår sammen df_peak_cost og total_monthly_cost basert på 'Month' kolonnen
    total_monthly_cost = pd.merge(df_peak_cost, total_monthly_cost, on="Month")

    total_monthly_cost.set_index('Month', inplace=True)

    # Total kostnad
    total_monthly_cost["total_cost"] = total_monthly_cost["Monthly_Peak_Cost"] + total_monthly_cost["energy_cost"]

    return total_monthly_cost
