import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
csv_file = 'callsigns.csv'

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

def reformat_table(table):  # returns 2 dataframes from the pivot table data. One Contesting, one WARC
    flattened = pd.DataFrame(table.to_records())
    # flattened['Zone'] = flattened['Zone'].apply(lambda x: f"{x} {zone_name_map.get(x, '')}")
    flattened.reset_index(drop=True)
    flattened1 = (flattened.reindex(
        ['Zone', '160', '80', '40', '20', '15', '10', '6'], axis=1).dropna(how='all', axis=1))
    flattened2 = (flattened.reindex(
        ['Zone', '30', '17', '12'], axis=1).dropna(how='all', axis=1))

    print(flattened1)
    print(flattened2)
    return flattened1, flattened2


def run():  # may make it so that function infinitely runs every hour or so
    df = pd.read_csv(csv_file, keep_default_na=False)
    # print(df.to_string())
    count_table = df.pivot_table(values='SNR', index=['Zone'], columns=['Band'], aggfunc='count')
    count_table = count_table.fillna(0)
    count_table = count_table.astype(int)
    contesting_counts, warc_counts = reformat_table(count_table)

    mean_table = df.pivot_table(values='SNR', index=['Zone'], columns=['Band'], aggfunc='mean')
    contesting_means, warc_means = reformat_table(mean_table)

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
    contesting_means_no_zone = contesting_means.drop(columns=['Zone'])
    color_table1 = contesting_means_no_zone.map(apply_color)

    warc_means_no_zone = warc_means.drop(columns=['Zone'])
    color_table2 = warc_means_no_zone.map(apply_color)

    # add the 'Zone' column back without applying the color map to it
    contesting_counts['Zone'] = contesting_means['Zone']
    warc_counts['Zone'] = warc_means['Zone']

    # apply the styles to the dataframes
    styled_table1 = contesting_counts.style.apply(lambda x: color_table1, axis=None).set_caption(
        "Contesting Bands")
    styled_table2 = warc_counts.style.apply(lambda x: color_table2, axis=None).set_caption(
        "WARC Bands")

    styled_table1.set_properties(subset=['Zone'], **{'font-weight': 'bold'})
    styled_table2.set_properties(subset=['Zone'], **{'font-weight': 'bold'})
    html1 = styled_table1.hide(axis="index").to_html()
    html2 = styled_table2.hide(axis="index").to_html()

    html_content = f"""
        <html>
        <head>
            <style>
                .container {{
                    display: flex;
                    flex-direction: row;
                    justify-content: space-around;
                }}
                .table-container {{
                    margin: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="table-container">
                    {html1}
                </div>
                <div class="table-container">
                    {html2}
                </div>
            </div>
        </body>
        </html>
        """

    with open("index.html", "w") as text_file:
        text_file.write(html_content)

    # with open("warc_index.html", "w") as text_file:
    #     text_file.write(html2)


if __name__ == '__main__':
    run()
