import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
csv_file = 'callsigns.csv'


def run():  # may make it so that function infinitely runs every hour or so
    df = pd.read_csv(csv_file, keep_default_na=False)
    # print(df.to_string())
    table = df.pivot_table(values='SNR', index=['Zone'], columns=['Band'], aggfunc='mean')

    print(table)

    cmap = sns.diverging_palette(240, 10, as_cmap=True)
    styled_table = table.style.background_gradient(cmap=cmap, axis=None).set_caption("test")

    html = styled_table.to_html()
    text_file = open("index.html", "w")
    text_file.write(html)
    text_file.close()


if __name__ == '__main__':
    run()
