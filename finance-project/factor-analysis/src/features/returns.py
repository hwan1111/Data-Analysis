import pandas as pd
import numpy as np

def add_cumulative_return(df: pd.DataFrame) -> pd.DataFrame:
    df['등락률_pct'] = df['등락률'] / 100 + 1
    df = df.sort_values(['종목코드', '날짜'])
    df['누적수익률'] = df.groupby('종목코드')['등락률_pct'].cumprod()
    return df
