import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
csv_file = 'callsigns.csv'


def run():  # may make it so that function infinitely runs every hour or so
    df = pd.read_csv(csv_file, keep_default_na=False)
    # print(df.to_string())
    count_table = df.pivot_table(values='SNR', index=['Zone'], columns=['Band'], aggfunc='count')
    count_table = count_table.fillna(0)
    count_table = count_table.astype(int)
    mean_table = df.pivot_table(values='SNR', index=['Zone'], columns=['Band'], aggfunc='mean')
    count_table.round(0)

    # print(table)

    # cmap = sns.diverging_palette(240, 10, as_cmap=True)
    # styled_table = table.style.background_gradient(cmap=cmap, axis=None).set_caption("test")

    colors = ["#FFFFFF", "#66CCFF", "#99FF99", "#FF9966"]
    n_bins = [0.0, 0.2, 0.6, 1.0]
    cmap_name = 'custom_heatmap'
    cmap = LinearSegmentedColormap.from_list(cmap_name, list(zip(n_bins, colors)))

    norm = plt.Normalize(vmin=mean_table.min().min(), vmax=mean_table.max().max())

    def apply_color(x):
        if pd.isna(x):
            return 'background-color: #f0f0f0'  # Slightly off-white (gray-ish)
        else:
            rgba = cmap(norm(x))
            return 'background-color: rgba({}, {}, {}, {})'.format(
                int(rgba[0] * 255), int(rgba[1] * 255), int(rgba[2] * 255), rgba[3]
            )

    styled_table = count_table.style.apply(
        lambda x: mean_table.map(apply_color), axis=None
    ).set_caption("Pivot Table Showing Counts Colored by Mean SNR")

    html = styled_table.to_html()
    text_file = open("index.html", "w")
    text_file.write(html)
    text_file.close()


if __name__ == '__main__':
    run()
