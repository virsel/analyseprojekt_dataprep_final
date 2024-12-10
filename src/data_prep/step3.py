import pandas as pd
import os
from ast import literal_eval
from parallel_pandas import ParallelPandas
from utils import comp_sent

# init parallel pandas
ParallelPandas.initialize(n_cpu=24)

dir_path = os.path.dirname(os.path.abspath(__file__))
in_path = os.path.join(dir_path, "../../output/{}_step2.csv")
out_path = os.path.join(dir_path, "../../output/{}_step3.csv")


def load(in_path):
    df = pd.read_csv(in_path)
    df['tweets'] = df['tweets'].apply(lambda x: literal_eval(x) if pd.notna(x) else [])
    return df

def main(name="stocks"):
    df = load(in_path.format(name))
    
    # Apply the sentiment analysis and expand the result into multiple columns
    sentiment_df = df['tweets'].p_apply(comp_sent).apply(pd.Series)
    
    # Combine the original DataFrame with the new sentiment columns
    df = pd.concat([df, sentiment_df], axis=1)
    df.to_csv(out_path.format(name), index=False)
    
if __name__ == '__main__':
    main()