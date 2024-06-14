import re
import telnetlib
import plistlib
from datetime import datetime, timedelta
import pandas as pd
import argparse

# host = "cluster.n2wq.com"
# port = 7373
# login = "LZ3NY"
pattern = r'(\d+\.\d{2})\s+([A-Z0-9/]+)\s+FT8\s+([+-]\s?\d{1,2})'
cty_file = "cty.plist"
pd.options.display.float_format = '{:.0f}'.format
csv_file = 'callsigns.csv'

callsign_df = pd.DataFrame()

parser = argparse.ArgumentParser()
parser.add_argument("-a", "--address",
                    help="Specify Telnet hostname/address. Default = cluster.n2wq.com", default="cluster.n2wq.com")
parser.add_argument("-p", "--port", help="Specify Telnet port. Default = 7373", type=int, default=7373)
parser.add_argument("-l", "--login", help="Specify login for cluster. Default = LZ3NY", default="LZ3NY")
parser.add_argument("-s", "--spotter",
                    help="Specify spotter name to track. Default = VE3EID", default="VE3EID")
parser.add_argument("-r", "--range", type=int, default=1,
                    help="Specify # of hours to store data before dropping. Default = 1")
args = parser.parse_args()
host = args.address
port = args.port
login = args.login
spotter = args.spotter
age = args.range


def search_list(call_sign, cty_list):
    while len(call_sign) >= 1 and call_sign not in cty_list:
        call_sign = call_sign[:-1]
    if len(call_sign) == 0:
        return None, None, None
    else:
        return cty_list[call_sign]["Continent"], cty_list[call_sign]["Country"], cty_list[call_sign]["CQZone"]


def calculate_band(freq):
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
    else:
        band = None
    return band


def delete_old(df):  # delete entries older than an hour from the dataframe
    day_ago = datetime.now().timestamp() - timedelta(hours=age).total_seconds()
    print(df[df['Timestamp'] <= day_ago].index)
    df = df.drop(df[df['Timestamp'] <= day_ago].index)
    return df


def run():
    global callsign_df

    with open(cty_file, 'rb') as infile:
        cty_list = plistlib.load(infile, dict_type=dict)

    tn = telnetlib.Telnet(host, port)
    tn.read_until(b'login: ')
    tn.write(login.encode() + b'\n')
    tn.write(b'SET/SKIMMER\nSET/NOCW\nSET/NOFT4\nSET/NORTTY\n')

    tn.read_until(b'DX')
    n = 0
    while True:
        data = tn.read_until(b'\n')

        try:  # I received a one-time UnicodeDecodeError when decoding -- never reoccurred. Added this preventatively.
            data = data.decode()
        except UnicodeDecodeError:
            print(data)
            continue

        spotter_string = spotter + "-#:"
        if "FT8" and spotter_string in data:
            time = datetime.now().timestamp()
            match = re.search(pattern, data)
            frequency = match.group(1) if match else None
            call_sign = match.group(2) if match else None
            snr = match.group(3).replace(" ", "") if match else None
            print(data)

            if match:  # when there's no match, the line of data is usually not usable, so I don't store it
                continent, country, cq_zone = search_list(call_sign, cty_list)
                band = calculate_band(float(frequency))
                if band:
                    temp_df = pd.DataFrame(
                        [{'Call Sign': call_sign, 'Continent': continent, 'Country': country, 'Zone': cq_zone,
                          'Frequency': frequency, 'Band': band, 'SNR': snr, 'Timestamp': time}])
                    callsign_df = pd.concat([callsign_df, temp_df], ignore_index=True)
            else: print(data)

        if n > 0 and n % 100 == 0:  # output data every 100 iters. if done every iter, output will be slower than input
            callsign_df = delete_old(callsign_df)
            callsign_df.to_csv(csv_file, index=False)  # writing dataframe minus old entries every iteration.
            print("iteration ", n)
        if n == 100000:
            n = 100
        n = n + 1

    # print(callsign_df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
