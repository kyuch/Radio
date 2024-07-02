import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
csv_file = 'callsigns.csv'

sparse = 5
busy = 10

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

    # flattened2 = (flattened.reindex(
    #     ['Zone', '30', '17', '12'], axis=1).dropna(how='all', axis=1))

    print(flattened1)
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
    # print(df.to_string())
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

    # warc_means_no_zone = warc_means.drop(columns=['Zone'])
    # color_table2 = warc_means_no_zone.map(apply_color)
    count_table = replace_values(count_table)
    # add the 'Zone' column back without applying the color map to it
    count_table['Zone'] = mean_table['Zone']
    count_table[' '] = mean_table[' ']
    # warc_counts['Zone'] = warc_means['Zone']


    # apply the styles to the dataframes
    styled_table1 = count_table.style.apply(lambda x: color_table1, axis=None).set_caption(
        "Band Activity Colored by Average SNR Value")
    # styled_table2 = warc_counts.style.apply(lambda x: color_table2, axis=None).set_caption(
    #     "WARC Bands")

    styled_table1.set_properties(subset=['Zone'], **{'font-weight': 'bold'})
    styled_table1.set_properties(**{'width': '30px'})
    styled_table1.set_properties(**{'text-align': 'center'})
    # styled_table2.set_properties(subset=['Zone'], **{'font-weight': 'bold'})

    styled_table1.set_table_styles([
        {'selector': 'th', 'props': [('font-size', '12pt')]},
        {'selector': 'td', 'props': [('font-size', '12pt')]},
    ])

    html1 = styled_table1.hide(axis="index").to_html()
    # html2 = styled_table2.hide(axis="index").to_html()

    with open("index.html", "w") as text_file:
        text_file.write(html1)

    # with open("warc_index.html", "w") as text_file:
    #     text_file.write(html2)


if __name__ == '__main__':
    run()
