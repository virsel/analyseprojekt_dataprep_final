import os
import pandas as pd

dir_path = os.path.dirname(os.path.abspath(__file__))

df_path = os.path.join(dir_path, "../../output/{}_step1.csv")
out_path = os.path.join(dir_path, "../../output/{}_step1.csv")


def merge(stocks, name="stocks"):
    dfs = []
    for stock in stocks:
        # Read the CSV and add a new column with the stock symbol
        temp_df = pd.read_csv(df_path.format(stock))
        temp_df['stock'] = stock  # Add a new column with the stock symbol
        dfs.append(temp_df)
    
    # Concatenate all dataframes
    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(out_path.format(name), index=False)