import re
import socket
import plistlib
from datetime import datetime, timedelta
import pandas as pd
import argparse
import os


# Regular expression pattern for filtering relevant lines from the DX Cluster data stream
pattern = r'(\d+\.\d{1,2})\s+([A-Z0-9/]+)\s+([+-]?\s?\d{1,2})\s*dB\s+\d+\s+(?:FT8|FT4|CW)' # new regex
# pattern = r'(\d+\.\d{2})\s+([A-Z0-9/]+)\s+(?:FT8|FT4|CW)\s+([+-]?\s?\d{1,2})\s*dB' # regex for filtering for desired lines from data stream

# Default file names
cty_file = "cty.plist"
csv_file = 'callsigns.csv'

# Pandas display options
pd.options.display.float_format = '{:.0f}'.format

# Create an empty dataframe to store callsigns
callsign_df = pd.DataFrame()


def search_list(call_sign, cty_list):
    """
    Search through the cty.plist file to find geographic information for the provided callsign.

    :param call_sign: The radio callsign being searched
    :param cty_list: The list containing geographic information for callsigns
    :return: Continent, Country, and CQZone of the callsign. Returns None if not found.
    """
    while len(call_sign) >= 1 and call_sign not in cty_list:
        call_sign = call_sign[:-1]
    if len(call_sign) == 0:
        return None
    else:
        return cty_list[call_sign]["CQZone"]


def calculate_band(freq):
    """
    Calculate the radio band category based on the frequency.

    :param freq: The frequency of the callsign
    :return: Radio band corresponding to the frequency, or None if no match is found
    """
    if 1800 <= freq <= 2000:
        return 160
    elif 3500 <= freq <= 4000:
        return 80
    elif 7000 <= freq <= 7300:
        return 40
    elif 10100 <= freq <= 10150:
        return 30
    elif 14000 <= freq <= 14350:
        return 20
    elif 18068 <= freq <= 18168:
        return 17
    elif 21000 <= freq <= 21450:
        return 15
    elif 24890 <= freq <= 24990:
        return 12
    elif 28000 <= freq <= 29700:
        return 10
    elif 50000 <= freq <= 54000:
        return 6
    return None


def delete_old(df):
    """
    Delete entries older than 6 hours from the dataframe to keep the data current.

    :param df: The dataframe to process
    :return: The dataframe without old entries
    """
    day_ago = datetime.now().timestamp() - timedelta(minutes=15).total_seconds()
    df = df.drop(df[df['Timestamp'] <= day_ago].index)
    return df


def run(host, port, spotter):
    """
    Main function to connect to the DX Cluster, receive and process data, and store it in a CSV file.

    :param host: The DX Cluster host address
    :param port: The DX Cluster port number
    :param spotter: The name of the spotter to track
    """
    global callsign_df
    last_time = time.time()

    # Load the cty.plist file with callsign information
    with open(cty_file, 'rb') as infile:
        cty_list = plistlib.load(infile, dict_type=dict)

    # Establish a socket connection to the DX Cluster
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))  # Connect to the DX cluster

    # s.sendall(b'LZ3NY\n')
    # s.sendall(b'SET/SKIMMER\nSET/NORTTY\nSET/FT4\nSET/FT8\nSET/CW\n')

    buffer = ""  # Buffer to store incoming data
    n = 0

    while True:
        try:
            data = s.recv(1024).decode()  # Receive data from the cluster
            buffer += data  # Append the received data to the buffer
        except UnicodeDecodeError as e:
            print(f"Decoding error: {e}")
            continue

        if not data:
            break  # Stop processing if the connection is closed

        # Split the buffer by newlines; the last part may be incomplete
        lines = buffer.split('\n')
        buffer = lines[-1]  # Save the incomplete line back to the buffer

        for line in lines[:-1]:  # Process all complete lines
            spotter_string = spotter + "-#:"
            if ((" FT4 " in line) or (" FT8 " in line)) and spotter_string in line:
                time = datetime.now().timestamp()  # Collect the timestamp of the data

                match = re.search(pattern, line)  # Match the line with the regex pattern

                if match:
                    frequency = match.group(1)
                    call_sign = match.group(2)
                    snr = match.group(3).replace(" ", "")
                    cq_zone = search_list(call_sign, cty_list)
                    band = calculate_band(float(frequency))

                    if band and cq_zone:
                        # Add the enhanced callsign info to the dataframe
                        temp_df = pd.DataFrame([{
                            'Call Sign': call_sign,
                            # 'Continent': continent,
                            # 'Country': country,
                            'Zone': cq_zone,
                            'Frequency': frequency,
                            'Band': band,
                            'SNR': snr,
                            'Timestamp': time,
                            'Spotter': spotter,
                            # 'CW': int(" CW " in line)
                        }])
                        callsign_df = pd.concat([callsign_df, temp_df], ignore_index=True)
                else:
                    print(f"No match: {line}")


        # Every 500 lines or 30 seconds, update the CSV file with the new info
        if ((n > 0 and n % 500 == 0) or (time.time() - last_time > 30)) and not callsign_df.empty:
            callsign_df = delete_old(callsign_df)  # Keep the CSV size manageable
            callsign_df.to_csv(csv_file, index=False)
            last_time = time.time()

            # Get the current time and print it along with the update message
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{csv_file} updated on {current_time}.")

        if n == 100000:
            n = 100  # Reset line counter to avoid overflow
        n += 1


if __name__ == '__main__':
    # Argument parser for command-line options
    parser = argparse.ArgumentParser(description="Connect to a DX Cluster, collect spotted callsigns, and store them in a CSV file.")
    parser.add_argument("-a", "--address", help="Specify hostname/address of the DX Cluster", default=os.getenv("DX_CLUSTER_HOST", "cluster.n2wq.com"))
    parser.add_argument("-p", "--port", help="Specify port for the DX Cluster", type=int, default=int(os.getenv("DX_CLUSTER_PORT", 7373)))
    parser.add_argument("-s", "--spotter", help="Specify the spotter name to track", default=os.getenv("SPOTTER_NAME", "VE3EID"))

    args = parser.parse_args()

    # Run the main function with provided arguments
    run(args.address, args.port, args.spotter)
