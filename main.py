import re
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


def run():
    stored_signs = {}

    with open(cty_file, 'rb') as infile:
        cty_list = plistlib.load(infile, dict_type=dict)

    tn = telnetlib.Telnet(host, port)
    tn.read_until(b'login: ')
    tn.write(login.encode() + b'\n')
    tn.write(b'SET/SKIMMER\nSET/NOCW\nSET/NOFT4\nSET/NORTTY\n')

    tn.read_until(b'DX')
    for n in range(1000):
        data = tn.read_until(b'\n').decode()
        if "FT8" in data:
            time = datetime.now().timestamp()
            match = re.search(pattern, data)
            frequency = match.group(1) if match else None
            call_sign = match.group(2) if match else None
            snr = match.group(3).replace(" ", "") if match else None
            print(data)
            # print(call_sign)
            # print(frequency)
            # print(snr)
            # print(time)

            continent, country, cq_zone = search_list(call_sign, cty_list)
            # print(continent)
            # print(country)
            # print(cq_zone)

            stored_signs[call_sign] = {'Continent': continent, 'Country': country, 'Zone': cq_zone, 'Frequency': frequency, 'SNR': snr, 'Timestamp': time}
            print(stored_signs[call_sign])

    # print(stored_signs)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
