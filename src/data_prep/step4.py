import pandas as pd
import os
from ast import literal_eval
from utils import comp_emb
from parallel_pandas import ParallelPandas

ParallelPandas.initialize(n_cpu=24)

dir_path = os.path.dirname(os.path.abspath(__file__))
in_path = os.path.join(dir_path, "../../output/{}_step3.csv")
out_path = os.path.join(dir_path, "../../output/{}_step4.csv")

def load(path):
    df = pd.read_csv(path)
    df['tweets'] = df['tweets'].apply(lambda x: literal_eval(x) if pd.notna(x) else [])
    return df

def process_tweet_column(df):
    """
    Processes the 'tweets' column of a DataFrame, masking elements in each tweet.
    
    Parameters:
    df (pd.DataFrame): DataFrame with a 'tweets' column containing lists of tweet texts
    
    Returns:
    pd.DataFrame: DataFrame with processed tweets
    """
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Process each list of tweets
    df['tweet_embs'] = df.tweets.p_apply(comp_emb)
    df.drop(columns=['tweets'], inplace=True)
    
    return df

def main(stock):
    df = load(in_path.format(stock))
    df['num_tweets'] = df['tweets'].apply(len)
    df2 = process_tweet_column(df)
    df2.to_csv(out_path.format(stock), index=False)
    
if __name__ == '__main__':
    main('stocks')







    


