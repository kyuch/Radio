import time
from datetime import datetime, timedelta
import pandas as pd
import argparse
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import requests
import xml.etree.ElementTree as ET


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
csv_file = 'callsigns.csv'

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--frequency", help="Specify how often data is collected (in minutes). Default = 1",
                    type=float, default=1)
parser.add_argument("-l", "--lower",
                    help="Specify the lower end of the data count threshold (empty square). Default = 5",
                    type=int, default=5)
parser.add_argument("-u", "--upper",
                    help="Specify the upper end of the data count threshold (filled square). Default = 10",
                    type=int, default=10)
parser.add_argument("-r", "--range", type=int, default=3,
                    help="Specify # of hours of data from current time to analyze. Default = 3")
args = parser.parse_args()
frequency = args.frequency
sparse = args.lower
busy = args.upper
span = args.range

# mapping zone numbers to descriptions...
zone_name_map = {
    1: 'Northwestern Zone of North America: KL (Alaska), VY1/VE8 Yukon, the Northwest and Nunavut Territories west of 102 degrees (Includes the islands of Victoria, Banks, Melville, and Prince Patrick).',
    2: 'Northeastern Zone of North America: VO2 Labrador, the portion of VE2 Quebec north of the 50th parallel, the VE8 Northwest and Nunavut Territories east of 102 degrees (Includes the islands of King Christian, King William, Prince of Wales, Somerset, Bathurst, Devon, Ellesmere, Baffin and the Melville and Boothia Peninsulas, excluding Akimiski Island).',
    3: 'Western Zone of North America: VE7, W6, and the W7 states of Arizona, Idaho, Nevada, Oregon, Utah, and Washington.',
    4: 'Central Zone of North America: VE3, VE4, VE5, VE6, VE8 Akimiski Island, and W7 states of Montana and Wyoming. W0, W9, W8 (except West Virginia), W5, and the W4 states of Alabama, Tennessee, and Kentucky.',
    5: 'Eastern Zone of North America: 4U1UN, CY9, CY0, FP, VE1, VE9, VY2, VO1 and the portion of VE2 Quebec south of the 50th parallel. VP9, W1, W2, W3 and the W4 states of Florida, Georgia, South Carolina, North Carolina, Virginia and the W8 state of West Virginia.',
    6: 'Southern Zone of North America: XE/XF, XF4 (Revilla Gigedo).',
    7: 'Central American Zone: FO (Clipperton), HK0 (San Andres and Providencia), HP, HR, TG, TI, TI9, V3, YN and YS.',
    8: 'West Indies Zone: C6, CO, FG, FJ, FM, FS, HH, HI, J3, J6, J7, J8, KG4 (Guantanamo), KP1, KP2, KP4, KP5, PJ (Saba, St. Maarten, St. Eustatius), V2, V4, VP2, VP5, YV0 (Aves Is.), ZF, 6Y, and 8P.',
    9: 'Northern Zone of South America: FY, HK, HK0 (Malpelo), P4, PJ (Bonaire, Curacao), PZ, YV, 8R, and 9Y.',
    10: 'Western Zone of South America: CP, HC, HC8, and OA.',
    11: 'Central Zone of South America: PY, PY0, and ZP.',
    12: 'Southwest Zone of South America: 3Y (Peter I), CE, CE0 (Easter Is., Juan Fernandez Is., San Felix Is.), and some Antarctic stations.',
    13: 'Southeast Zone of South America: CX, LU, VP8 Islands, and some Antarctic stations.',
    14: 'Western Zone of Europe: C3, CT, CU, DL, EA, EA6, El, F, G, GD, GI, GJ, GM. GU, GW, HB, HB0, LA, LX, ON, OY, OZ, PA, SM, ZB, 3A and 4U1ITU.',
    15: 'Central European Zone: ES, HA, HV, I, IS0, LY, OE, OH, OH0, OJ0, OK, OM, S5, SP, T7, T9, TK, UA2, YL, YU, ZA, 1A0, Z3, 9A, 9H and 4U1VIC.',
    16: 'Eastern Zone of Europe: UR-UZ, EU-EW, ER, UA1, UA3, UA4, UA6, UA9 (S, T, W), and R1MV (Malyj Vysotskij).',
    17: 'Western Zone of Siberia: EZ, EY, EX, UA9 (A, C, F, G, J, K, L, M, Q, X) UK, UN-UQ, UH, UI and UJ-UM.',
    18: 'Central Siberian Zone: UA8 (T, V), UA9 (H, O, U, Y, Z), and UA0 (A, B, H, O, S, U, W).',
    19: 'Eastern Siberian Zone: UA0 (C, D, E, I, J, K, L, Q, X, Z).',
    20: 'Balkan Zone: E4, JY, LZ, OD, SV, SV5, SV9, SV/A, TA, YK, YO, ZC4, 4X and 5B.',
    21: 'Southwestern Zone of Asia: 4J, 4K, 4L, A4, A6, A7, A9, AP, EK, EP, HZ, YA, YI, 7O and 9K.',
    22: 'Southern Zone of Asia: A5, S2, VU, VU (Lakshadweep Is.), 4S, 8Q, and 9N.',
    23: 'Central Zone of Asia: JT, UA0Y, BY3G-L (Nei Mongol), BY9, BY0.',
    24: 'Eastern Zone of Asia: BQ9 (Pratas), BV, BY1, BY2, BY3A-F (Tian Jin), BY3M-R (He Bei), BY3S-X (Shan Xi), BY4, BY5, BY6, BY7, BY8, VR and XX.',
    25: 'Japanese Zone: HL, JA and P5.',
    26: 'Southeastern Zone of Asia: HS, VU (Andaman and Nicobar Islands), XV(3W), XU, XW, XZ and 1S (Spratly Islands).',
    27: 'Philippine Zone: DU (Philippines), JD1 (Minami Torishima), JD1 (Ogasawara), T8(KC6) (Palau), KH2 (Guam), KH0 (Marianas Is.), V6 (Fed. States of Micronesia) and BS7 (Scarborough Reef).',
    28: 'Indonesian Zone: H4, P2, V8, YB, 4W (East Timor), 9M and 9V.',
    29: 'Western Zone of Australia: VK6, VK8, VK9X (Christmas Is.), VK9C (Cocos-Keeling Is.) and some Antarctic stations.',
    30: 'Eastern Zone of Australia: FK/C (Chesterfield), VK1-5, VK7, VK9L (Lord Howe Is.), VK9W (Willis Is.), VK9M (Mellish Reef), VK0 (Macquarie Is.) and some Antarctic stations.',
    31: 'Central Pacific Zone: C2, FO (Marquesas), KH1, KH3, KH4, KH5, KH5K, KH6, KH7, KH7K, KH9, T2, T3, V7 and ZK3.',
    32: 'New Zealand Zone: A3, FK (except Chesterfield), FO (except Marquesas and Clipperton), FW, H40(Temotu), KH8, VK9N (Norfolk Is.) VP6 (Pitcairn and Ducie), YJ, ZK1, ZK2, ZL, ZL7, ZL8, 3D2, 5W and some Antarctic stations.',
    33: 'Northwestern Zone of Africa: CN, CT3, EA8, EA9, IG9, IH9 (Pantelleria Is.), S0, 3V and 7X.',
    34: 'Northeastern Zone of Africa: ST, SU and 5A.',
    35: 'Central Zone of Africa: C5, D4, EL J5, TU, TY, TZ, XT, 3X, 5N, 5T, 5U, 5V, 6W, 9G and 9L.',
    36: 'Equatorial Zone of Africa: D2, TJ, TL, TN, S9, TR, TT, ZD7, ZD8, 3C, 3C0, 9J, 9Q, 9U and 9X.',
    37: 'Eastern Zone of Africa: C9, ET, E3, J2, T5, 5H, 5X, 5Z, 7O and 7Q.',
    38: 'South African Zone: A2, V5, ZD9, Z2, ZS1-ZS8, 3DA, 3Y (Bouvet Is.), 7P, and some Antarctic stations.',
    39: 'Madagascar Zone: D6, FT-W, FT-X, FT-Z, FH, FR, S7, VK0 (Heard Is.) VQ9, 3B6/7, 3B8, 3B9, 5R8 and some Antarctic stations.',
    40: 'North Atlantic Zone: JW, JX, OX, R1FJ (Franz Josef Land), and TF.'
}

def get_aws_credentials():
    """
    Prompts the user to input their AWS credentials.

    :return: A dictionary containing 'aws_access_key_id' and 'aws_secret_access_key'
    """
    access_key = input("Enter your AWS Access Key ID: ")
    secret_key = input("Enter your AWS Secret Access Key: ")
    bucket = input("Enter the name of the S3 Bucket you'd like to write to: ")

    return {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
        's3_bucket': bucket
    }


def upload_file_to_s3(file_name, bucket_name, acc_key, sec_key):
    creds = {
        'aws_access_key_id': acc_key,
        'aws_secret_access_key': sec_key
    }
    s3_client = boto3.client('s3', **creds)
    obj_name = 'index.html'

    try:
        s3_client.upload_file(file_name, bucket_name, obj_name, ExtraArgs={'ContentType':'text/html; charset=utf-8'})
        print(f"File {file_name} uploaded successfully to {bucket_name}/{obj_name}")
        return True
    except FileNotFoundError:
        print(f"The file {file_name} was not found")
    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials provided")
    except Exception as e:
        print(f"An error occurred: {e}")

    return False


def reformat_table(table):
    flattened = pd.DataFrame(table.to_records())
    flattened['Zone'] = flattened['Zone'].apply(lambda x: f'<span title="{zone_name_map.get(x, "")}">{str(x).zfill(2)}</span>')
    flattened.reset_index(drop=True)
    flattened1 = (flattened.reindex(
        ['Zone', '160', '80', '40', '20', '15', '10', '6', ' ', '30', '17', '12'], axis=1))

    flattened1 = flattened1.fillna({' ': ' '})

    return flattened1


def delete_old(df):  # delete entries older than the range from the dataframe
    day_ago = datetime.now().timestamp() - timedelta(hours=span).total_seconds()
    print(df[df['Timestamp'] <= day_ago].index)
    df = df.drop(df[df['Timestamp'] <= day_ago].index)
    return df


def replace_values(df):
    df = df.fillna(0)

    def replace_value(x):
        if isinstance(x, (int, float)):
            if x == 0:
                return ' '
            elif x <= sparse:
                return '◻'
            elif sparse < x < busy:
                return '◩'
            elif x >= busy:
                return '◼'
        return x

    return df.map(replace_value)


def run(access_key, secret_key, s3_buck):
    df = pd.read_csv(csv_file, keep_default_na=False)
    spotter = df['Spotter'].iloc[0]
    df = delete_old(df)  # ignores any data older than range from the csv.
    count_table = df.pivot_table(values='SNR', index=['Zone'], columns=['Band'], aggfunc='count')
    count_table = count_table.fillna(0)
    count_table = count_table.astype(int)
    count_table = reformat_table(count_table)

    mean_table = df.pivot_table(values='SNR', index=['Zone'], columns=['Band'], aggfunc='mean')
    mean_table = reformat_table(mean_table)

    def apply_color(val):
        if pd.isna(val):
            return 'background-color: #f0f0f0'
        elif val <= -15:
            return 'background-color: #a3cce9'
        elif -15 < val <= -10:
            return 'background-color: #b6e3b5'
        elif -10 < val <= -3:
            return 'background-color: #f7c896'
        else:
            return 'background-color: #e57373'

    # apply color map to numeric columns only
    means_no_zone = mean_table.drop(columns=['Zone', ' '])
    color_table1 = means_no_zone.map(apply_color)

    count_table = replace_values(count_table)
    # add the 'Zone' column back without applying the color map to it
    count_table['Zone'] = mean_table['Zone']
    count_table[' '] = mean_table[' ']

    now = datetime.utcnow().strftime("%b %d, %Y %H:%M:%S")
    caption_string = "Conditions at " + spotter + " as of " + now + " GMT"

    # apply the styles to the dataframes
    styled_table1 = count_table.style.apply(lambda x: color_table1, axis=None).set_caption(caption_string)

    styled_table1.set_properties(subset=['Zone'], **{'font-weight': 'bold'})
    styled_table1.set_properties(**{'text-align': 'center'})

    # set table styles to different parts of the table
    styled_table1.set_table_styles([
        {'selector': 'caption', 'props': [('font-size', '13pt'), ('font-weight', 'bold')]},
        {'selector': 'th',
         'props': [('font-size', '12pt'), ('word-wrap', 'break-word'), ('position', 'sticky'),
                   ('top', '0'), ('background-color', 'rgba(255, 255, 255, 0.75)'), ('z-index', '1')]},
        {'selector': 'td:first-child', 'props': [('font-size', '12pt')]},  # First column
        {'selector': 'td',
         'props': [('font-size', '10pt'), ('padding-left', 'calc(5px + 1vw)'), ('padding-top', '4px'), ('padding-bottom', '4px'), ('padding-right', 'calc(5px + 1vw)')]}
    ])

    # convert the styled table to HTML
    html1 = styled_table1.hide(axis="index").to_html()

    html1 = html1.replace('<table ',
                          '<table style="width: 60vw; table-layout: fixed; margin-left: auto; margin-right: auto;" ')
    # html1 = html1.replace('<thead>',
    #                       '<thead><colgroup><col style="width: 118px;"></colgroup>')

    # legend HTML
    legend_html = f"""
     <head> <meta http-equiv="refresh" content="60"> </head> 
     <div style="position: fixed; bottom: 0; left: 50%; transform: translateX(-50%); width: 90%; background-color: rgba(255, 255, 255, 0.75); font-weight: bold; padding: 10px; border: 1px solid gray; box-sizing: border-box; z-index: 1000;">
         <div style="display: flex; justify-content: space-around; margin-bottom: 10px;">
             <div style="display: flex; align-items: center; margin-right: 20px; font-size: 12pt;">
                 <div style="width: 20px; height: 20px; background-color: #a3cce9; margin-right: 5px;"></div>
                 <div>Marginal (≤ -15 dB)</div>
             </div>
             <div style="display: flex; align-items: center; margin-right: 20px; font-size: 12pt;">
                 <div style="width: 20px; height: 20px; background-color: #b6e3b5; margin-right: 5px;"></div>
                 <div>Normal (-15 to -10 dB)</div>
             </div>
             <div style="display: flex; align-items: center; margin-right: 20px; font-size: 12pt;">
                 <div style="width: 20px; height: 20px; background-color: #f7c896; margin-right: 5px;"></div>
                 <div>Above Average (-10 to -3 dB)</div>
             </div>
             <div style="display: flex; align-items: center; font-size: 12pt;">
                 <div style="width: 20px; height: 20px; background-color: #e57373; margin-right: 5px;"></div>
                 <div>Hot (> -3 dB)</div>
             </div>
         </div>
         <div style="display: flex; justify-content: space-around;">
             <div style="display: flex; align-items: center; margin-right: 20px; font-size: 12pt;">
                 <div style="font-size: 20px; margin-right: 5px;">◻</div>
                 <div>Quiet (≤ {sparse} spots)</div>
             </div>
             <div style="display: flex; align-items: center; margin-right: 20px; font-size: 12pt;">
                 <div style="font-size: 20px; margin-right: 5px;">◩</div>
                 <div>Moderate ({sparse + 1} to {busy - 1} spots)</div>
             </div>
             <div style="display: flex; align-items: center; font-size: 12pt;">
                 <div style="font-size: 20px; margin-right: 5px;">◼</div>
                 <div>Busy (≥ {busy} spots)</div>
             </div>
         </div>
     </div>
     """

    # fetch solar widget XML data
    solar_response = requests.get("https://www.hamqsl.com/solarxml.php")
    xml_data = solar_response.content
    root = ET.fromstring(xml_data)

    # extract relevant data
    solar_data = {
        "SFI": root.findtext("solardata/solarflux"),
        "Sunspots": root.findtext("solardata/sunspots"),
        "A-Index": root.findtext("solardata/aindex"),
        "K-Index": root.findtext("solardata/kindex"),
        "X-Ray": root.findtext("solardata/xray"),
        "Signal Noise": root.findtext("solardata/signalnoise"),
    }

    # extract and organize conditions for each band, day, and night
    conditions = {
        "80m-40m": {"Day": "", "Night": ""},
        "30m-20m": {"Day": "", "Night": ""},
        "17m-15m": {"Day": "", "Night": ""},
        "12m-10m": {"Day": "", "Night": ""},
    }

    # fill in the day and night conditions for each band
    for band in root.findall("solardata/calculatedconditions/band"):
        band_name = band.get("name")
        time = band.get("time")
        condition = band.text
        if band_name in conditions:
            conditions[band_name][time.capitalize()] = condition

    # Create the HTML table
    solar_table_html = """
    <div style="width: 100%; text-align: center; font-weight: bold; margin-bottom: 5px;">Solar-Terrestrial Data</div>
    <hr>
    <div style="display: flex; justify-content: center; margin-bottom: 5px;">
        <div style="margin-right: 20px;">
            <span style="font-weight: bold;">SFI:</span> <span style="font-weight: bold;">{SFI}</span>
        </div>
        <div>
            <span style="font-weight: bold;">SN:</span> <span style="font-weight: bold;">{Sunspots}</span>
        </div>
    </div>
    <div style="display: flex; justify-content: center; margin-bottom: 5px;">
    <div style="flex: 1; text-align: center;">
        <span style="font-weight: bold;">A:</span> <span style="font-weight: bold;">{A-Index}</span>
    </div>
    <div style="flex: 1; text-align: center;">
        <span style="font-weight: bold;">K:</span> <span style="font-weight: bold;">{K-Index}</span>
    </div>
    <div style="flex: 1; text-align: center;">
        <span style="font-weight: bold;">X:</span> <span style="font-weight: bold;">{X-Ray}</span>
    </div>
</div>
    <hr>
    <div style="width: 100%; text-align: center; font-weight: bold; margin-bottom: 5px;">Calculated Conditions</div>
    <table style="width: 60%; margin: 0 auto; border-collapse: collapse;">
        <thead>
            <tr>
                <th style="padding: 5px; text-align: center; font-weight: bold;">Band</th>
                <th style="padding: 5px; text-align: center; font-weight: bold;">Day</th>
                <th style="padding: 5px; text-align: center; font-weight: bold;">Night</th>
            </tr>
        </thead>
        <tbody>
    """

    # Add the conditions for each band with color coding
    for band, condition in conditions.items():
        day_condition = condition.get("Day", "N/A")
        night_condition = condition.get("Night", "N/A")
        day_color = "green" if day_condition == "Good" else "red"
        night_color = "green" if night_condition == "Good" else "red"

        solar_table_html += f"""
        <tr>
            <td style="padding: 5px; text-align: center; font-weight: bold; white-space: nowrap;">{band}</td>
            <td style="padding: 5px; text-align: center; font-weight: bold; color: {day_color}; white-space: nowrap;">{day_condition}</td>
            <td style="padding: 5px; text-align: center; font-weight: bold; color: {night_color}; white-space: nowrap;">{night_condition}</td>
        </tr>
        """

    # Close the table
    solar_table_html += """
        </tbody>
    </table>
    <div style="margin-top: 5px; text-align: center">
            <span style="font-weight: bold;">Signal Noise:</span> <span style="font-weight: bold;">{Signal Noise}</span>
        </div>
    """

    # Insert the solar data values into the HTML template
    solar_table_html = solar_table_html.format(**solar_data)

    # Wrap the table in a fixed div
    solar_table_html = f"""
    <div style="position: fixed; left: 5%; top: 0px; padding: 10px; z-index: 1000; font-family: 'Roboto', monospace;">
        {solar_table_html}
    </div>
    """

    # HTML for the solar widget
    # solar_widget_html = """
    # <div style="position: fixed; left: 0; top: 0; padding: 10px; z-index: 1000;">
    #     <a href="https://www.hamqsl.com/solar.html" title="Click to add Solar-Terrestrial Data to your website!">
    #         <img src="https://www.hamqsl.com/solar100sc.php">
    #     </a>
    # </div>
    # """

    # Final HTML
    final_html = f"""
    <style>
        body {{
            margin: 0;
            padding: 0;
            overflow-y: hidden; /* Prevent vertical scrolling */
        }}
        html {{
            height: 100%;
        }}
    </style>
    <div style="display: flex; width: 100%; height: 100%;">
        {solar_table_html}
        <div style="position: relative; flex-grow: 1; padding-left: 110px; overflow-y: auto; font-family: 'Roboto', monospace;"> <!-- Added padding to accommodate the table -->
            <div style="max-height: 80vh; overflow-y: auto; padding-top: 0.75%; padding-bottom: 10%;"> <!-- This div creates the scrollable area -->
                {html1}
            </div>
            <div>{legend_html}</div>
        </div>
    </div>
    """

    with open("index.html", "w", encoding="utf-8") as text_file:
        text_file.write(final_html)

    with open("index.html", "w", encoding="utf-8") as text_file:
        text_file.write(final_html)

    print("Table updated in index.html at " + now)
    upload_file_to_s3("index.html", s3_buck, access_key, secret_key)


if __name__ == '__main__':
    time_to_wait = frequency * 60
    credentials = get_aws_credentials()
    aws_access_key = credentials['aws_access_key_id']
    secret_access_key = credentials['aws_secret_access_key']
    s3_bucket = credentials['s3_bucket']

    while True:
        run(aws_access_key, secret_access_key, s3_bucket)
        time.sleep(time_to_wait)