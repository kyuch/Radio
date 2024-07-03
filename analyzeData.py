import time
from datetime import datetime
import pandas as pd
import argparse


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
args = parser.parse_args()
frequency = args.frequency
sparse = args.lower
busy = args.upper


# keeping this in case I have to name zones by callsign
# zone_name_map = {
#     1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five',
#     6: 'six', 7: 'seven', 8: 'eight', 9: 'nine', 10: 'ten',
#     11: 'eleven', 12: 'twelve', 13: 'thirteen', 14: 'fourteen',
#     15: 'fifteen', 16: 'sixteen', 17: 'seventeen', 18: 'eighteen',
#     19: 'nineteen', 20: 'twenty', 21: 'twenty-one', 22: 'twenty-two',
#     23: 'twenty-three', 24: 'twenty-four', 25: 'twenty-five', 26: 'twenty-six',
#     27: 'twenty-seven', 28: 'twenty-eight', 29: 'twenty-nine', 30: 'thirty',
#     31: 'thirty-one', 32: 'thirty-two', 33: 'thirty-three', 34: 'thirty-four',
#     35: 'thirty-five', 36: 'thirty-six', 37: 'thirty-seven', 38: 'thirty-eight',
#     39: 'thirty-nine', 40: 'forty'
# }


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


def run():  # may make it so that function infinitely runs every hour or so
    df = pd.read_csv(csv_file, keep_default_na=False)
    spotter = df['Spotter'].iloc[0]
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
        {'selector': 'th', 'props': [('font-size', '12pt')]},
        {'selector': 'td', 'props': [('font-size', '12pt')]},
    ])

    html1 = styled_table1.hide(axis="index").to_html()

    legend_html = f"""
    <div style="margin-top: 20px; margin-left: 40px; padding: 10px; border: 1px solid black; width: 200px;">
        <h3 style="text-align: center;">Legend</h3>
        <div style="display: flex; align-items: center;">
            <div style="width: 20px; height: 20px; background-color: #a3cce9; margin-right: 10px;"></div>
            <div>Cold (≤ -15 dB)</div>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="width: 20px; height: 20px; background-color: #b6e3b5; margin-right: 10px;"></div>
            <div>Cool (-15 to -10 dB)</div>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="width: 20px; height: 20px; background-color: #f7c896; margin-right: 10px;"></div>
            <div>Warm (-10 to -3 dB)</div>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="width: 20px; height: 20px; background-color: #e57373; margin-right: 10px;"></div>
            <div>Hot (> -3 dB)</div>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="font-size: 20px; margin-right: 10px;">◻</div>
            <div>Sparse (≤ {sparse} callsigns)</div>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="font-size: 16px; margin-right: 10px;">◩</div>
            <div>Moderate ({sparse + 1} to {busy - 1} callsigns)</div>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="font-size: 20px; margin-right: 10px;">◼</div>
            <div>Busy (≥ {busy} callsigns)</div>
        </div>
    </div>
    """


    final_html = f"""
        <div style="display: flex;">
            <div>{html1}</div>
            <div>{legend_html}</div>
        </div>
        """

    with open("index.html", "w") as text_file:
        text_file.write(final_html)

    print("Table updated at index.html at " + now)


if __name__ == '__main__':
    time_to_wait = frequency * 3600
    while True:
        run()
        time.sleep(time_to_wait)