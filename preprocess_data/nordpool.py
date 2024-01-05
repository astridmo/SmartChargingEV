"""
File to get Day-Ahead prices for NO1 from Nordpool. The data is stored in their FTP.
"""

from ftplib import FTP
import config
from datetime import date
import pandas as pd
from scipy.stats import norm
import numpy as np
import calendar

host = "ftp.nordpoolgroup.com"
username = config.nordpool_username
password = config.nordpool_password

year_dict = {"2018": "18", "2019": "19", "2020": "20", "2021": "21", "2022": "22", "2023": "23"}  # Valid years

today = date.today()


def get_nordpool_prices(year):
    """
    Function to get Day-Ahead prices for the given year from Nordpool. NB! The returning Excel-file must be processed
    in order to use it in analysis.

    :param: year: The year that is studied.
    :return: xls-file
    """
    if year not in year_dict.keys():
        raise ValueError(f"Wrong input. The year must be one of these values: {year_dict.keys()}.")

    remote_filename = f"oslo{year_dict[year]}.xls"
    local_filename = f"../assets/nordpool/unprocessed/NO1_{year_dict[year]}_{today}.xls"

    # Connect to the FTP server
    ftp = FTP(host)
    ftp.login(username, password)

    if year != "2023":
        # Navigate into the Day-Ahead price directory for the year (not 2023)
        ftp.cwd(config.nordpool_dir+'/'+year)
    else:
        # Navigate into the Day-Ahead price directory for 2023
        ftp.cwd(config.nordpool_dir)

    # Download 2023 data
    with open(local_filename, "wb") as local_file:
        ftp.retrbinary("RETR " + remote_filename, local_file.write)

    ftp.quit()


def create_spotprice_df(include_2030=False):
    """
    Function to process the Day-Ahead-prices from Nordpool and save it in a csv with DateTimeUTC as index and price as
    value. The Day-Ahead input data is in UTC (it is transformed from CET to UTC at an earlier step)
    :param include_2030: If 'True' a generated dataset from 2030 is included
    :return: df_price: DataFrame of spot prices
    """
    df_price = pd.DataFrame()
    for year in year_dict.keys():
        df_price_temp = pd.read_csv(f"../assets/nordpool/processed/{year}_processed.csv", sep=";", decimal=",")

        # Drop columns where the whole price row is 0 (which means that there is no data because the date is in the future)
        df_price_temp = df_price_temp[df_price_temp[df_price_temp.columns[1:]].sum(axis=1) != 0]

        # If the last two hours in the last day is 0 (i.e. no data until tomorrow)
        if df_price_temp.iloc[-1, -2:].sum() == 0:
            df_price_temp.drop(df_price_temp.tail(1).index, inplace=True)  # drop last row

        df_price_temp = df_price_temp.rename(columns={'Unnamed: 0': 'Date'})  # Rename Date-column

        # Make new dataframe where the date and time is columns while the value is price. I.e. only 3 columns
        df_price_temp = df_price_temp.melt(id_vars=["Date"],
                                           var_name="Time",
                                           value_name="Price [NOK/MWh]")
        # Create datetime
        df_price_temp['DateTimeUtc'] = pd.to_datetime(df_price_temp['Date'] + ' ' + df_price_temp['Time'],
                                                      format="%d/%m/%Y %H.%M.%S")
        df_price_temp = df_price_temp.drop(['Date', 'Time'], axis=1)  # Drop unnecessary columns
        df_price_temp = df_price_temp.set_index('DateTimeUtc')  # Set DateTime to index
        df_price_temp = df_price_temp.sort_values(by='DateTimeUtc')

        df_price = pd.concat([df_price, df_price_temp])  # Append to df_price

    if include_2030:
        df_spot_price_2030, df_utfallsrom = get_2030_spotprice()
        df_price = pd.concat([df_price, df_spot_price_2030])

    df_price.to_csv(f"../assets/nordpool/spot_prices_{date.today()}.csv")

    return df_price


def get_2030_spotprice():
    """
    Function to create spot price data for 2030.
    :return: df_spotprice: DataFrame of hourly generated spot prices for 2030
             df_utfallsrom: DataFrame of the parameters used to make the spot prices
    """
    utfallsrom = {  # i kr/MWh
        'januar': {'Mean': 960, 'STD': 140},
        'februar': {'Mean': 970, 'STD': 130},
        'mars': {'Mean': 930, 'STD': 110},
        'april': {'Mean': 760, 'STD': 190},
        'mai': {'Mean': 650, 'STD': 270},
        'juni': {'Mean': 600, 'STD': 300},
        'juli': {'Mean': 620, 'STD': 280},
        'august': {'Mean': 660, 'STD': 250},
        'september': {'Mean': 760, 'STD': 190},
        'oktober': {'Mean': 810, 'STD': 140},
        'november': {'Mean': 900, 'STD': 100},
        'desember': {'Mean': 960, 'STD': 140},
    }

    months_map = {
        'januar': 1, 'februar': 2, 'mars': 3,
        'april': 4, 'mai': 5, 'juni': 6,
        'juli': 7, 'august': 8, 'september': 9,
        'oktober': 10, 'november': 11, 'desember': 12
    }

    # DataFrame to save hourly calculated values
    df_spot_price_2030 = pd.DataFrame()

    for month_name, values in utfallsrom.items():
        month_number = months_map[month_name]
        # Set seed for reproducability
        np.random.seed(month_number + 1)

        # Make DataTime-values
        year = 2030
        start_date = f"{year}-{month_number:02d}-01 00:00"
        end_day = calendar.monthrange(year, month_number)[1]
        end_date = f"{year}-{month_number:02d}-{end_day} 23:00"
        date_range = pd.date_range(start=start_date, end=end_date, freq='H')

        # Create temporart DataFrame for this month
        temp_df = pd.DataFrame(date_range, columns=['DateTimeUtc'])
        num_hours = len(temp_df)  # Number of hours in the month

        # Create artificial dataset for this month by using normal distribution
        mean = values['Mean']  # Find average
        std_dev = values['STD']  # Find standard deviation
        temp_df['Price [NOK/MWh]'] = norm.rvs(loc=mean, scale=std_dev, size=num_hours)  # Generate dataset

        # Add prices to the DataFrame for spotprices in 2030
        df_spot_price_2030 = pd.concat([df_spot_price_2030, temp_df])

    # Set 'Datetime' columns as index
    df_spot_price_2030.set_index('DateTimeUtc', inplace=True)

    df_utfallsrom = pd.DataFrame(utfallsrom).T  # Transpose to get the months as index

    return df_spot_price_2030, df_utfallsrom
