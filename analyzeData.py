import pandas as pd

csv_file = 'callsigns.csv'


def run():  # may make it so that function infinitely runs every hour or so
    df = pd.read_csv(csv_file)
    print(df.to_string())


if __name__ == '__main__':
    run()
