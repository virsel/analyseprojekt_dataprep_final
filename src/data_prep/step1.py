import pandas as pd
import os
import json
from pathlib import Path
from datetime import datetime

dir_path = os.path.dirname(os.path.abspath(__file__))
price_path = os.path.join(dir_path, "../../input/price/{}.csv")
tweets_path = os.path.join(dir_path, "../../input/tweet/{}")
output_path = os.path.join(dir_path, "../../output/{}_step1.csv")

def load_prices(price_path):
    df = pd.read_csv(price_path)
    df.columns = df.columns.str.lower()
    return df

def load_tweets(folder_path):
    """
    Parse tweet JSON files from a folder into a pandas DataFrame.
    Each file contains tweets from one day, with multiple tweets possible per day.
    
    Parameters:
    folder_path (str): Path to the folder containing tweet JSON files
    
    Returns:
    pandas.DataFrame: DataFrame with 'date' and 'text' columns
    """
    all_tweets = []
    
    # Convert folder path to Path object for better cross-platform compatibility
    folder = Path(folder_path)
    
    # Iterate through all files in the folder
    for file_path in folder.glob('*'):
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read file line by line as each line is a separate JSON object
            for line in f:
                if line.strip():  # Skip empty lines
                    try:
                        tweet = json.loads(line)
                        
                        # Parse the created_at date
                        date = datetime.strptime(
                            tweet['created_at'], 
                            '%a %b %d %H:%M:%S +0000 %Y'
                        ).strftime('%Y-%m-%d')
                        
                        all_tweets.append({
                            'date': date,
                            'text': tweet['text']
                        })
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"Error processing tweet in {file_path}: {e}")
                        continue
    
    # Create DataFrame from all tweets
    df = pd.DataFrame(all_tweets)
    
    # Sort by date
    df = df.sort_values('date')
    
    return df

def merge_price_tweet(df_price, df_tweet):
    # Get the min and max dates from tweet DataFrame
    min_tweet_date = df_tweet['date'].min()
    max_tweet_date = df_tweet['date'].max()

    # Filter price DataFrame to only include dates within the tweet date range
    df_price = df_price[
        (df_price['date'] >= min_tweet_date) & 
        (df_price['date'] <= max_tweet_date)
    ]
    
    # Create a sorted list of all price dates
    price_dates = sorted(df_price['date'].unique())
    
    # Function to find next available price date
    def find_next_price_date(tweet_date):
        # Find the first price date that comes on or after the tweet date
        future_dates = [d for d in price_dates if d >= tweet_date]
        return future_dates[0] if future_dates else price_dates[-1]
    
    # Align tweet dates with next available price date
    df_tweet['aligned_date'] = df_tweet['date'].apply(find_next_price_date)
    
    # Group tweets by aligned date and aggregate texts into lists
    tweet_groups = (df_tweet.groupby('aligned_date')['text']
                   .agg(lambda x: list(x))
                   .reset_index()
                   .rename(columns={'text': 'tweets'}))
    
    # Merge with price data
    merged_df = pd.merge(
        df_price,
        tweet_groups,
        left_on='date',
        right_on='aligned_date',
        how='left'
    )
    
    # Fill missing tweet lists with empty lists
    merged_df['tweets'] = merged_df['tweets'].apply(lambda x: [] if isinstance(x, float) and pd.isna(x) else x)
    
    # Drop the redundant aligned_date column and sort by date
    merged_df = merged_df.drop('aligned_date', axis=1).sort_values('date')
    
    return merged_df

def main(stock):
    df_tweet = load_tweets(tweets_path.format(stock)) 
    df_price = load_prices(price_path.format(stock))  
    df_merged = merge_price_tweet(df_price, df_tweet)
    df_merged.to_csv(output_path.format(stock), index=False)
    return df_merged





    


