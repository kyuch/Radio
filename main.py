import re
import telnetlib
import plistlib
host = "cluster.n2wq.com"
port = 7373
login = "LZ3NY"
frequency_pattern = r'(\d+\.\d{2})'
callsign_pattern = r'([A-Z0-9]{3,6})'
snr_pattern = r'([+-]\s?\d{1,2})\s+dB'
pattern = r'(\d+\.\d{2})\s+([A-Z0-9/]+)\s+FT8\s+([+-]\s?\d{1,2})\s+dB'



def run():
    tn = telnetlib.Telnet(host, port)
    tn.read_until(b'login: ')
    tn.write(login.encode() + b'\n')
    tn.write(b'SET/SKIMMER\n')
    tn.write(b'SET/NOCW\n')
    tn.write(b'SET/NOFT4\n')
    tn.write(b'SET/NORTTY\n')

    tn.read_until(b'DX')
    for n in range(150):
        data = tn.read_until(b'\n').decode()
        if "FT8" in data:
            match = re.search(pattern, data)
            frequency = match.group(1) if match else None
            call_sign = match.group(2) if match else None
            snr = match.group(3).replace(" ", "") if match else None
            print(data)
            print(call_sign)
            print(frequency)
            print(snr)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
