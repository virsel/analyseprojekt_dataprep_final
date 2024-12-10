import pandas as pd
import os
import re
from pathlib import Path
from datetime import datetime, timedelta
from ast import literal_eval

dir_path = os.path.dirname(os.path.abspath(__file__))
in_path = os.path.join(dir_path, "../../output/{}_step1.csv")
out_path = os.path.join(dir_path, "../../output/{}_step2.csv")

def load(path):
    df = pd.read_csv(path)
    df['tweets'] = df['tweets'].apply(lambda x: literal_eval(x) if pd.notna(x) else [])
    return df

def mask_tweet_text(text):
    """
    Masks URLs, @mentions, and $stock_labels in tweet text.
    
    Parameters:
    text (str): Original tweet text
    
    Returns:
    str: Masked tweet text
    """
    # URL pattern - matches http(s)://, www., and common domain patterns
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    
    # First mask URLs
    text = re.sub(url_pattern, 'URL', text)
    
    # Then mask @ mentions (including any connected text until space)
    text = re.sub(r'@\w+', 'AT_ENTITY', text)
    
    # Finally mask $stock mentions (convert to uppercase)
    def stock_replacer(match):
        stock = match.group(1)
        # Check if the stock string contains any digit
        if any(char.isdigit() for char in stock):
            return f'${stock}'  # Return original if any digit is present
        else:
            return f'{stock.upper()} stock'
    
    text = re.sub(r'\$(\w+)', stock_replacer, text)
    
    # Remove special characters except for normal punctuation (.,!?) and %,$
    # Also remove quotes (" or ')
    text = re.sub(r'[^\w\s.,!?$%]', '', text)
    
    # Remove extra whitespace (more than one space)
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


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
    df['tweets'] = df['tweets'].apply(
        lambda tweet_list: [mask_tweet_text(tweet) for tweet in tweet_list]
    )
    
    return df


def main(stock):
    df = load(in_path.format(stock))
    df2 = process_tweet_column(df)
    df2.to_csv(out_path.format(stock), index=False)





    


