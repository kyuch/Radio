"""
   processData.py connects to a DX Cluster, collects all spotted callsigns from a provided spotter, 
   enhances the data for each spotted callsign, and uploads the enhanced data to a csv file.

   To run, pip install pandas, boto3.
   Author: Alex Kyuchukov
""" 


import re
import socket
import plistlib
from datetime import datetime, timedelta
import pandas as pd
import argparse
import boto3

client = boto3.client('s3')

pattern = r'(\d+\.\d{2})\s+([A-Z0-9/]+)\s+(?:FT8|FT4)\s+([+-]\s?\d{1,2})'  # regex for filtering for desired lines from data stream
cty_file = "cty.plist"
pd.options.display.float_format = '{:.0f}'.format
csv_file = 'callsigns.csv'

callsign_df = pd.DataFrame()

parser = argparse.ArgumentParser()  # argument parser
parser.add_argument("-a", "--address",
                    help="Specify hostname/address. Default = cluster.n2wq.com", default="cluster.n2wq.com")
parser.add_argument("-p", "--port", help="Specify port. Default = 7373", type=int, default=7373)
parser.add_argument("-l", "--login", help="Specify login for cluster. Default = LZ3NY", default="LZ3NY")
parser.add_argument("-s", "--spotter",
                    help="Specify spotter name to track. Default = VE3EID", default="VE3EID")

args = parser.parse_args()
host = args.address
port = args.port
login = args.login
spotter = args.spotter


def search_list(call_sign, cty_list):
    """
    search_list searches through the cty.plist file to find a match for the provided callsign and provide more info.

    :param call_sign: The radio callsign we are searching for
    :param cty_list: The file containing our callsign information.
    :return: The continent, country, and CQ zone of our callsign. Returns None if callsign not found.
    """ 
    while len(call_sign) >= 1 and call_sign not in cty_list:
        call_sign = call_sign[:-1]
    if len(call_sign) == 0:
        return None, None, None
    else:
        return cty_list[call_sign]["Continent"], cty_list[call_sign]["Country"], cty_list[call_sign]["CQZone"]


def calculate_band(freq):
    """
    calculate_band calculates the radio band category when provided the callsign's frequency.

    :param freq: The frequency of a callsign.
    :return: The radio band of the callsign. None if there is no match.
    """ 
    if 1800 <= freq <= 2000:  # between 160 and 6, skipping 6 and 60
        band = 160
    elif 3500 <= freq <= 4000:
        band = 80
    elif 7000 <= freq <= 7300:
        band = 40
    elif 10100 <= freq <= 10150:
        band = 30
    elif 14000 <= freq <= 14350:
        band = 20
    elif 18068 <= freq <= 18168:
        band = 17
    elif 21000 <= freq <= 21450:
        band = 15
    elif 24890 <= freq <= 24990:
        band = 12
    elif 28000 <= freq <= 29700:
        band = 10
    elif 50000 <= freq <= 54000:
        band = 6
    else:
        band = None
    return band


def delete_old(df):
    """
    delete_old deletes entries older than a specified age from the dataframe. Default is 1 day.

    :param df: The dataframe containing our collected data.
    :return: The dataframe without the older data.
    """ 
    day_ago = datetime.now().timestamp() - timedelta(days=1).total_seconds()
    # print(df[df['Timestamp'] <= day_ago].index)
    df = df.drop(df[df['Timestamp'] <= day_ago].index)
    return df


def run():
    global callsign_df

    with open(cty_file, 'rb') as infile:  # load cty_list file
        cty_list = plistlib.load(infile, dict_type=dict)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    s.connect((host, port))  # connect to the DX cluster

    s.sendall(f"{login}\n".encode())
    s.sendall(b'SET/SKIMMER\nSET/NOCW\nSET/NORTTY\n')

    n = 0
    while True:  # continue reading from DX cluster
        try:
            data = s.recv(1024).decode()  # read a line from the cluster
        except UnicodeDecodeError as e:  # if for some reason a line can't be decoded with utf-8, just skip thru it.
            print(e)
            print(data)

        if not data:
            break  # handle the case where the connection is closed

        spotter_string = spotter + "-#:"
        if ("FT8" or "FT4") and spotter_string in data:  # ensure that we are reading data from our spotter.
            time = datetime.now().timestamp()  # collect timestamp of data
            match = re.search(pattern, data)  # use regex to ensure  formatting of data is readable
            frequency = match.group(1) if match else None
            call_sign = match.group(2) if match else None
            snr = match.group(3).replace(" ", "") if match else None

            if match:
                continent, country, cq_zone = search_list(call_sign, cty_list)  # search the cty_list for enhanced callsign data
                band = calculate_band(float(frequency))
                if band:  # add enhanced callsign info to dataframe
                    temp_df = pd.DataFrame(  
                        [{'Call Sign': call_sign, 'Continent': continent, 'Country': country, 'Zone': cq_zone,
                          'Frequency': frequency, 'Band': band, 'SNR': snr, 'Timestamp': time, 'Spotter': spotter}])
                    callsign_df = pd.concat([callsign_df, temp_df], ignore_index=True)
            else:
                print(data)

        if n > 0 and n % 100 == 0 and not callsign_df.empty:  # overwrite csv file with new info every 100 lines read. 
            callsign_df = delete_old(callsign_df)  # delete old info from dataframe. This keeps info current and csv file size small.
            callsign_df.to_csv(csv_file, index=False)  # convert dataframe to csv file.
            print(csv_file + " updated.")
        if n == 100000:
            n = 100
        n = n + 1


if __name__ == '__main__':
    run()
