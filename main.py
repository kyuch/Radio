import re
import csv
import itertools  # for csv.DictWriter
import telnetlib
import plistlib
from datetime import datetime

host = "cluster.n2wq.com"
port = 7373
login = "LZ3NY"
pattern = r'(\d+\.\d{2})\s+([A-Z0-9/]+)\s+FT8\s+([+-]\s?\d{1,2})'
cty_file = "cty.plist"


def search_list(call_sign, cty_list):
    while len(call_sign) >= 1 and call_sign not in cty_list:
        call_sign = call_sign[:-1]
    if len(call_sign) == 0:
        return None, None, None
    else:
        return cty_list[call_sign]["Continent"], cty_list[call_sign]["Country"], cty_list[call_sign]["CQZone"]


def convert_to_csv(dictionary):
    fields = ['Call Sign', 'Continent', 'Country', 'Zone', 'Frequency', 'Band', 'SNR', 'Timestamp']
    with open('callsigns.csv', 'w', newline='') as file:
        w = csv.DictWriter(file, fields)
        w.writeheader()
        for k in dictionary:
            w.writerow({field: dictionary[k].get(field) or k for field in fields})


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


def run():
    stored_signs = {}

    with open(cty_file, 'rb') as infile:
        cty_list = plistlib.load(infile, dict_type=dict)

    tn = telnetlib.Telnet(host, port)
    tn.read_until(b'login: ')
    tn.write(login.encode() + b'\n')
    tn.write(b'SET/SKIMMER\nSET/NOCW\nSET/NOFT4\nSET/NORTTY\n')

    tn.read_until(b'DX')
    for n in range(1500):
        data = tn.read_until(b'\n')

        try:  # I received a one-time UnicodeDecodeError when decoding -- never reoccurred. Added this preventatively.
            data = data.decode()
        except UnicodeDecodeError:
            print(data)
            continue

        if "FT8" in data:
            time = datetime.now().timestamp()
            match = re.search(pattern, data)
            frequency = match.group(1) if match else None
            call_sign = match.group(2) if match else None
            snr = match.group(3).replace(" ", "") if match else None
            # print(data)

            if match:  # when there's no match, the line of data is usually not usable, so I don't store it
                continent, country, cq_zone = search_list(call_sign, cty_list)
                band = calculate_band(float(frequency))
                if band:
                    stored_signs[call_sign] = {'Continent': continent, 'Country': country, 'Zone': cq_zone,
                                               'Frequency': frequency, 'Band': band, 'SNR': snr, 'Timestamp': time}
                    # print(stored_signs[call_sign])

    # print(stored_signs)
    convert_to_csv(stored_signs)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
