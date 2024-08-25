import time
from datetime import datetime, timedelta
import pandas as pd
import argparse
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
csv_file = 'callsigns.csv'

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--frequency", help="Specify how often data is collected (in hours). Default = 1",
                    type=int, default=1)
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
    timestamp = datetime.now().strftime('%m.%d.%Y-%H:%M:%S')
    obj_name = 'radio_chart_' + timestamp + '.html'

    try:
        s3_client.upload_file(file_name, bucket_name, obj_name)
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
    # flattened['Zone'] = flattened['Zone'].apply(lambda x: f"{x} {zone_name_map.get(x, '')}")
    flattened.reset_index(drop=True)
    # flattened['Zone '] = flattened['Zone']
    # flattened['Zone '] = flattened.loc[:, 'Zone']
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
    styled_table1 = count_table.style.apply(lambda x: color_table1, axis=None).set_caption(
        caption_string)

    styled_table1.set_properties(subset=['Zone'], **{'font-weight': 'bold'})
    styled_table1.set_properties(**{'width': '30px'})
    styled_table1.set_properties(**{'text-align': 'center'})

    styled_table1.set_table_styles([
        {'selector': 'th', 'props': [('font-size', '10pt')]},
        {'selector': 'td', 'props': [('font-size', '10pt')]},
    ])

    html1 = styled_table1.hide(axis="index").to_html()

    legend_html = f"""
        <div style="margin-top: 10px; padding: 10px; border: 1px solid black; width: fit-content; margin-left: auto; margin-right: auto;">
            <div style="display: flex; justify-content: space-around; margin-bottom: 10px;">
                <div style="display: flex; align-items: center; margin-right: 20px; font-size: 10pt;">
                    <div style="width: 20px; height: 20px; background-color: #a3cce9; margin-right: 5px;"></div>
                    <div>Marginal (≤ -15 dB)</div>
                </div>
                <div style="display: flex; align-items: center; margin-right: 20px; font-size: 10pt;">
                    <div style="width: 20px; height: 20px; background-color: #b6e3b5; margin-right: 5px;"></div>
                    <div>Normal (-15 to -10 dB)</div>
                </div>
                <div style="display: flex; align-items: center; margin-right: 20px; font-size: 10pt;">
                    <div style="width: 20px; height: 20px; background-color: #f7c896; margin-right: 5px;"></div>
                    <div>Above Average (-10 to -3 dB)</div>
                </div>
                <div style="display: flex; align-items: center; font-size: 10pt;">
                    <div style="width: 20px; height: 20px; background-color: #e57373; margin-right: 5px;"></div>
                    <div>Hot (> -3 dB)</div>
                </div>
            </div>
            <div style="display: flex; justify-content: space-around;">
                <div style="display: flex; align-items: center; margin-right: 20px; font-size: 10pt;">
                    <div style="font-size: 20px; margin-right: 5px;">◻</div>
                    <div>Quiet (≤ {sparse} spots)</div>
                </div>
                <div style="display: flex; align-items: center; margin-right: 20px; font-size: 10pt;">
                    <div style="font-size: 20px; margin-right: 5px;">◩</div>
                    <div>Moderate ({sparse + 1} to {busy - 1} spots)</div>
                </div>
                <div style="display: flex; align-items: center; font-size: 10pt;">
                    <div style="font-size: 20px; margin-right: 5px;">◼</div>
                    <div>Busy (≥ {busy} spots)</div>
                </div>
            </div>
        </div>
        """

    final_html = f"""
        <div style="display: flex; flex-direction: column; align-items: center;">
            <div>{html1}</div>
            <div>{legend_html}</div>
        </div>
        """

    with open("index.html", "w") as text_file:
        text_file.write(final_html)

    print("Table updated at index.html at " + now)
    upload_file_to_s3("index.html", s3_buck, access_key, secret_key)


if __name__ == '__main__':
    time_to_wait = frequency * 3600
    credentials = get_aws_credentials()
    aws_access_key = credentials['aws_access_key_id']
    secret_access_key = credentials['aws_secret_access_key']
    s3_bucket = credentials['s3_bucket']

    while True:
        run(aws_access_key, secret_access_key, s3_bucket)
        time.sleep(time_to_wait)