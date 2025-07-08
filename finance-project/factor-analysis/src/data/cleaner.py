import os
import json
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from tqdm import tqdm

def drop_nan(df, excluded_tickers_path='data/excluded_tickers.json'):
    with open(excluded_tickers_path) as file:
        excluded_tickers = set(json.load(file)['excluded_tickers'])

    df = df[~df['종목코드'].isin(excluded_tickers)].reset_index(drop=True)
    df['등락률'] = df['등락률'].fillna(0)
    return df
