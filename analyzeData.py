import time
import datetime as dt
from datetime import datetime, timedelta
import pandas as pd
import argparse
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import requests
import xml.etree.ElementTree as ET
import htmlmin
import sqlite3


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)

parser = argparse.ArgumentParser()  # argument parser
parser.add_argument("-f", "--frequency", help="Specify how often data is collected (in minutes). Default = 1",
                    type=float, default=1)
parser.add_argument("-l", "--lower",
                    help="Specify the lower end of the data count threshold (empty square). Default = 5",
                    type=int, default=5)
parser.add_argument("-u", "--upper",
                    help="Specify the upper end of the data count threshold (filled square). Default = 10",
                    type=int, default=10)
parser.add_argument("-r", "--range", type=int, default=0.25,
                    help="Specify # of hours of data from current time to analyze. Default = 0.25")
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
    12: 'Southwest Zone of South America: 3Y (Peter I), CE, CE0 (Easter Is., Juan Fernandez Is.), and some Antarctic stations.',
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
    Prompts the user to input their AWS credentials and the bucket they'd like to upload to.

    :return: A dictionary containing 'aws_access_key_id', 'aws_secret_access_key', and 's3_bucket'.
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
    """
    Uploads the html file to the AWS S3 bucket.

    :param file_name: The name of the html file being uploaded.
    :param bucket_name: The name of the bucket being uploaded to.
    :param acc_key: The AWS access key for S3 access.
    :param sec_key: The secret access key for the AWS access key.
    :return: Boolean True if the file was uploaded successfully. False if not uploaded successfully.
    """ 
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
    quit(1)


def reformat_table(table):
    """
    Reformats our pivot table into a dataframe convenient for HTML display.

    :param table: The pivot table being reformatted.
    :return: A dataframe reformatted from the pivot table.
    """
    flattened = pd.DataFrame(table.to_records())
    flattened['zone'] = pd.to_numeric(flattened['zone'], errors='coerce')
    flattened = flattened.sort_values(by='zone')
    flattened['zone'] = flattened['zone'].apply(lambda x: f'<span title="{zone_name_map.get(x, "")}">{str(x).zfill(2)}</span>')
    flattened.reset_index(drop=True)
    flattened1 = (flattened.reindex(
        ['zone', '160', '80', '40', '20', '15', '10', '6', ' ', '30', '17', '12'], axis=1))


    flattened1 = flattened1.fillna({' ': ' '})

    return flattened1


def delete_old(df, time):
    """
    Deletes entries older than the range from the dataframe.

    :param df: The dataframe being modified.
    :param time: The range of age before deletion
    :return: The dataframe without the older entries.
    """
    df = df[df['timestamp'].apply(lambda x: isinstance(x, (int, float)))]
    df = df.reset_index(drop=True)
    day_ago = datetime.now().timestamp() - timedelta(hours=time).total_seconds()
    df = df.drop(df[df['timestamp'] <= day_ago].index)

    return df


def replace_values(df):
    """
    Replaces numbers in the dataframe with symbols, replaces NaN with an empty space.

    :param df: The dataframe being modified.
    :return: A dataframe with modified entries.
    """
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


def update_count_table(count_df, cw_df):
    """
    Updates the count dataframe by appending a '+' wherever the CW table has a value >= 1.

    :param count_df: The dataframe containing counts (SNR).
    :param cw_df: The dataframe containing CW counts.
    :return: Modified count_df with '+' added where necessary.
    """
    count_df = count_df.fillna(0)  # fill NaNs with 0 for consistency in count_df
    cw_df = cw_df.fillna(0)  # fill NaNs with 0 in cw_df as well

    for col in count_df.columns:
        if col in cw_df.columns:  # make sure the columns match
            for i in range(len(count_df)):
                try:
                    if cw_df[col].iloc[i] >= 1:  # check if CW table has 1 or more
                        count_df.loc[i, col] = f"{count_df.loc[i, col]}+"  # append '+' in count table
                finally:
                    continue

    return count_df


def run(access_key, secret_key, s3_buck):
    # Connect to the SQLite database
    conn = sqlite3.connect('callsigns.db')
    
    # Read data from the SQLite table `callsigns` into a pandas DataFrame
    query = """
    SELECT zone, band, snr, timestamp, spotter
    FROM callsigns
    """
    
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Error: Unable to read data from the SQLite database. {e}")
        return
    finally:
        conn.close()  # Close the database connection after reading the data

    spotter = df['spotter'].iloc[0]
    df = delete_old(df, span)  # ignore any data older than range from the database.

    count_table = df.pivot_table(values='snr', index=['zone'], columns=['band'], aggfunc='count')  # pivot based on Zones and Bands with snr being the value.
    count_table = count_table.fillna(0)
    count_table = count_table.astype(int)
    count_table = reformat_table(count_table)

    mean_table = df.pivot_table(values='snr', index=['zone'], columns=['band'], aggfunc='mean')  # turn dataframe into pivot table.
    mean_table = reformat_table(mean_table)

    def apply_color(val):  # colors cells based on if the zone/Bands are Hot or Marginal.
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

    # apply color map to numeric columns only.
    means_no_zone = mean_table.drop(columns=['zone', ' '])
    color_table1 = means_no_zone.map(apply_color)

    count_table = replace_values(count_table)

    # add the 'zone' column back without applying the color map to it.
    count_table['zone'] = mean_table['zone']
    count_table[' '] = mean_table[' ']

    now = dt.datetime.now(dt.timezone.utc).strftime("%b %d, %Y %H:%M")
    caption_string = "Realtime Spots - " + spotter + " - " + now + " GMT"  # table caption

    # apply the styles to the dataframes.
    styled_table1 = count_table.style.apply(lambda x: color_table1, axis=None).set_caption(caption_string)

    styled_table1.set_properties(subset=['zone'], **{'font-weight': 'bold'})
    styled_table1.set_properties(**{'text-align': 'center'})

    # set table styles to different parts of the table.
    styled_table1.set_table_styles([
        {'selector': 'caption', 'props': [('font-size', '13pt'), ('font-weight', 'bold')]},
        {'selector': 'th',
         'props': [('font-size', '12pt'), ('word-wrap', 'break-word'), ('position', 'sticky'),
                   ('top', '0'), ('background-color', 'rgba(255, 255, 255, 0.75)'), ('z-index', '1')]},
        {'selector': 'td:first-child', 'props': [('font-size', '12pt')]},  # First column
        {'selector': 'td',
         'props': [('font-size', '10pt'), ('padding-left', 'calc(5px + 1vw)'), ('padding-top', '4px'), ('padding-bottom', '4px'), ('padding-right', 'calc(5px + 1vw)')]}
    ])

    # convert the styled table to HTML.
    html1 = styled_table1.hide(axis="index").to_html()

    html1 = html1.replace('<table ',
                          '<table style="width: 60vw; table-layout: fixed; margin-left: auto; margin-right: auto;" ')

    # legend HTML block.
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

    # fetch solar widget XML data.
    solar_response = requests.get("https://www.hamqsl.com/solarxml.php")
    xml_data = solar_response.content
    root = ET.fromstring(xml_data)

   # Extract solar and band condition data
    solar_data = {
        "SFI": root.findtext("solardata/solarflux"),
        "Sunspots": root.findtext("solardata/sunspots"),
        "A-Index": root.findtext("solardata/aindex"),
        "K-Index": root.findtext("solardata/kindex"),
        "X-Ray": root.findtext("solardata/xray"),
        "Signal_Noise": root.findtext("solardata/signalnoise"),  # Replace space with an underscore for key safety
        "Aurora": root.findtext("solardata/aurora"),
        "Lat.": root.findtext("solardata/latdegree"),
    }

    conditions = {
        "80m-40m": {"Day": "", "Night": ""},
        "30m-20m": {"Day": "", "Night": ""},
        "17m-15m": {"Day": "", "Night": ""},
        "12m-10m": {"Day": "", "Night": ""},
    }

    for band in root.findall("solardata/calculatedconditions/band"):
        band_name = band.get("name")
        time = band.get("time")
        condition = band.text
        if band_name in conditions:
            conditions[band_name][time.capitalize()] = condition

    # HTML content
    solar_table_html = f"""
    <div style="width: 100%; text-align: center; font-weight: bold; margin-bottom: 5px;">Solar Data by N0NBH</div>
    <hr>
    <div style="display: flex; justify-content: center; margin-bottom: 5px;">
        <div style="margin-right: 20px;">
            <span style="font-weight: bold;">SFI:</span> <span style="font-weight: bold;">{solar_data['SFI']}</span>
        </div>
        <div>
            <span style="font-weight: bold;">SSN:</span> <span style="font-weight: bold;">{solar_data['Sunspots']}</span>
        </div>
    </div>
<div style="display: inline-flex; justify-content: center; align-items: center; width: 100%; text-align: center; white-space: nowrap;">
    <div style="margin-right: 10px;">
        <span style="font-weight: bold;">A:</span> <span style="font-weight: bold;">{solar_data['A-Index']}</span>
    </div>
    <div style="margin-right: 10px;">
        <span style="font-weight: bold;">K:</span> <span style="font-weight: bold;">{solar_data['K-Index']}</span>
    </div>
    <div>
        <span style="font-weight: bold;">X:</span> <span style="font-weight: bold;">{solar_data['X-Ray']}</span>
    </div>
</div>

<div style="display: flex; justify-content: center; align-items: center; white-space: nowrap;">
    <div style="margin-right: 20px;">
        <span style="font-weight: bold;">Aurora:</span> <span style="font-weight: bold;">{solar_data['Aurora']}</span>
    </div>
    <div>
        <span style="font-weight: bold;">Lat.:</span> <span style="font-weight: bold;">{solar_data['Lat.']}</span>
    </div>
</div>

    <hr>
    <div style="width: 100%; text-align: center; font-weight: bold; margin-top: 10px;">Band Conditions</div>
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

 # add the conditions for each band with color coding.
    for band, condition in conditions.items():
        day_condition = condition.get("Day", "N/A")
        night_condition = condition.get("Night", "N/A")
        if day_condition == "Good":
            day_color = "green"
        elif day_condition == "Fair":
            day_color = "orange"
        else:
            day_color = "red"
        if night_condition == "Good":
            night_color = "green"
        elif night_condition == "Fair":
            night_color = "orange"
        else:
            night_color = "red"

        solar_table_html += f"""
        <tr>
            <td style="padding: 5px; text-align: center; font-weight: bold; white-space: nowrap;">{band}</td>
            <td style="padding: 5px; text-align: center; font-weight: bold; color: {day_color}; white-space: nowrap;">{day_condition}</td>
            <td style="padding: 5px; text-align: center; font-weight: bold; color: {night_color}; white-space: nowrap;">{night_condition}</td>
        </tr>
        """

    solar_table_html += """
        </tbody>
    </table>
    <div style="margin-top: 5px; text-align: center">
        <span style="font-weight: bold;">Signal Noise:</span> <span style="font-weight: bold;">{Signal_Noise}</span>
    </div>
    """

    # Formatting the placeholders directly in `solar_table_html`
    solar_table_html = solar_table_html.format(
        SFI=solar_data['SFI'],
        Sunspots=solar_data['Sunspots'],
        A_Index=solar_data['A-Index'],
        K_Index=solar_data['K-Index'],
        X_Ray=solar_data['X-Ray'],
        Signal_Noise=solar_data['Signal_Noise'],
        Aurora=solar_data['Aurora'],
        Lat=solar_data['Lat.']
    )

    # wrap the table in a fixed div.
    solar_table_html = f"""
    <div style="position: fixed; left: 5%; padding-top: 0.75%; padding: 10px; z-index: 1000; font-family: 'Roboto', monospace;">
        {solar_table_html}
    </div>
    """

    final_html = f"""
    <style>
        body {{
            margin: 0;
            padding: 0;
            overflow-y: hidden;
        }}
        html {{
            height: 100%;
        }}
    </style>
    <div style="display: flex; width: 100%; height: 100%;">
        {solar_table_html}
        <div style="position: relative; flex-grow: 1; padding-left: 160px; overflow-y: auto; font-family: 'Roboto', monospace;">
            <div style="max-height: 80vh; overflow-y: auto; padding-top: 0.75%; padding-bottom: 10%;">
                <!-- Add your main table content here (html1) -->
            </div>
        </div>
    </div>
    """


       # Final HTML block with solar table, main table, and legend html blocks all together.
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
        <div style="position: relative; flex-grow: 1; padding-left: 120px; overflow-y: auto; font-family: 'Roboto', monospace;"> <!-- Added padding to accommodate the table -->
            <div style="max-height: 80vh; overflow-y: auto; padding-top: 0.75%; padding-bottom: 10%;"> <!-- This div creates the scrollable area -->
                {html1}
            </div>
            <div>{legend_html}</div>
        </div>
    </div>
    """

    # Minify the final_html before writing it to the file
    minified_html = htmlmin.minify(final_html, remove_empty_space=True, remove_comments=True)

    # Write the minified HTML to index.html
    with open("index.html", "w", encoding="utf-8") as text_file:  # write minified HTML data to index.html file.
        text_file.write(minified_html)

    print("Table updated in index.html at " + now)
    
    upload_file_to_s3("index.html", s3_buck, access_key, secret_key)  # upload index.html to S3 bucket


if __name__ == '__main__':
    time_to_wait = frequency * 60  # time to wait in between re-running program
    credentials = get_aws_credentials()
    aws_access_key = credentials['aws_access_key_id']
    secret_access_key = credentials['aws_secret_access_key']
    s3_bucket = credentials['s3_bucket']

    while True:  # run program every 'n' minutes, which will re-analyze data and upload a new index.html to the S3 bucket.
        run(aws_access_key, secret_access_key, s3_bucket)
        time.sleep(time_to_wait)
