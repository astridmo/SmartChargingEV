from datetime import date
import pandas as pd
import requests
import config


def get_chargers(token):
    """
    Use Zaptec API to get data of chargers

    :return: csv-file of chargers with name chargers+date
    """
    url = "https://api.zaptec.com/api/chargers"
    r = requests.get(url=url, headers={"Authorization": f"Bearer {token}"})  # Get data from API
    if 200 <= r.status_code < 300:
        raw_df = pd.DataFrame(r.json()["Data"])
    else:  # Throw exception if something goes wrong
        HTMLResponseError = "HTML query returned an unexpected response. Status_code: " + str(r.status_code)
        raise Exception(HTMLResponseError)

    df = raw_df[["Id", "DeviceId", "Name", "CreatedOnDate", "CircuitId"]]
    df = df.replace({'Name': {'-': '', 'P': 'p', 'R': 'r', ' ': '_'}}, regex=True)

    today = date.today()
    df.to_csv(f"../assets/chargers_{today}.csv")


def get_chargehistory(chargers_path, token):
    """
    Use Zaptec API to get data of charge history
    :param chargers_path: The path for the csv containing constant charger information
    :return: csv-file of chargehistory for all chargers
    """
    base_url = "https://api.zaptec.com/api/chargehistory"

    chargers_df = pd.read_csv(chargers_path)

    raw_df = pd.DataFrame()

    for charger_id in chargers_df['Id']:

        url = f"{base_url}?ChargerId={charger_id}"

        r = requests.get(url=url, headers={"Authorization": f"Bearer {token}"})  # Get data from API

        if 200 <= r.status_code < 300:
            temp_df = pd.DataFrame(r.json()["Data"])
            raw_df = pd.concat([raw_df, temp_df])
        else:  # Throw exception if something goes wrong
            HTMLResponseError = "HTML query returned an unexpected response. Status_code: " + str(r.status_code)
            print("Error with the request:", r)
            raise Exception(HTMLResponseError)

    today = date.today()
    raw_df.to_csv(f"../assets/chargehistory_{today}.csv")


def get_token():
    """
    File to get a token from Zaptec. The token will be used to get data from their API.
    """
    # Define the url and the data that will be sent with the url
    url = "https://api.zaptec.com/oauth/token"
    data = {
        'grant_type': 'password',
        'username': config.username,  # My username
        'password': config.password  # My password
    }

    # Send POST-request
    response = requests.post(url, data=data)

    # Check if the response is successful
    if response.status_code == 200:
        # Get the access_token from the response
        access_token = response.json().get('access_token')
        print("Access token:", access_token)
    else:
        print("Error with the request:", response.text)

    return access_token
