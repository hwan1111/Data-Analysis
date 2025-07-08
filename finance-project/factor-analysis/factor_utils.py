import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from tqdm import tqdm
from sklearn.preprocessing import MinMaxScaler, RobustScaler, StandardScaler

from pykrx import stock as pkstock
from pykrx import bond as pkbond
import yfinance

def update_kospi_ohlcv(filepath="kospi_ohlcv.parquet"):
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y%m%d')

    # 기존 데이터 로드
    if os.path.exists(filepath):
        df_all = pd.read_parquet(filepath)
        print(f"OHLCV 기존 파일 로드됨: {len(df_all):,} rows")

        latest_date = df_all['날짜'].max().date()
        if latest_date >= yesterday:
            print("이미 최신 데이터까지 포함되어 있음.")
            return df_all
        start = latest_date + timedelta(days=1)
    else:
        df_all = pd.DataFrame()
        start = today - timedelta(days=365)

    start_str = start.strftime('%Y%m%d')

    # 종목 리스트
    tickers = pkstock.get_market_ticker_list(market="KOSPI")
    all_data = []

    for ticker in tqdm(tickers, desc=f"종목별 OHLCV 수집"):
        try:
            df = pkstock.get_market_ohlcv_by_date(start_str, yesterday_str, ticker).reset_index()
            if df.empty:
                continue

            df['종목코드'] = ticker
            df['종목명'] = pkstock.get_market_ticker_name(ticker)

            df = df[[
                '날짜',
                '종목코드',
                '종목명',
                '종가'
            ]]

            all_data.append(df)
        except Exception as e:
            print(f"[{ticker}] 에러 발생: {e}")

    if not all_data:
        print("수집된 데이터 없음.")
        return df_all

    df_new = pd.concat(all_data, ignore_index=True)
    df_new['날짜'] = pd.to_datetime(df_new['날짜'])

    # 기존과 병합 후 중복 제거
    df_all = pd.concat([df_all, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=['날짜', '종목코드'], keep='last', inplace=True)

    # 등락률 재계산
    df_all.sort_values(['종목코드', '날짜'], inplace=True)
    df_all['등락률'] = df_all.groupby('종목코드')['종가'].pct_change() * 100

    df_all.sort_index(inplace=True)

    # 저장
    df_all.to_parquet(filepath)
    print(f"{yesterday_str}까지 반영 완료. 총 {len(df_all):,} rows → {filepath}")

    return df_all

def update_kospi_fundamental(
        filepath='kospi_fundamental.parquet',
        json_path='excluded_tickers.json'):

    with open(json_path, 'r') as file:
        EXCLUDED_TICKERS = set(json.load(file)['excluded_tickers'])

    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y%m%d')

    # 기존 데이터 로드
    if os.path.exists(filepath):
        df_all = pd.read_parquet(filepath)
        print(f"Fundamental 기존 파일 로드됨: {len(df_all):,} rows")

        latest_date = df_all['날짜'].max().date()
        if latest_date >= yesterday:
            print('이미 최신 데이터까지 포함되어 있음.')
            return df_all
        start = latest_date + timedelta(days=1)
    else:
        df_all = pd.DataFrame()
        start = today - timedelta(days=365)

    start_str = start.strftime('%Y%m%d')

    # 종목 리스트
    tickers = pkstock.get_market_ticker_list(market='KOSPI')
    all_data = []

    for ticker in tqdm(tickers, desc=f'종목별 재무재표 수집'):
        try:
            if ticker in EXCLUDED_TICKERS:
                continue

            df = pkstock.get_market_fundamental_by_date(start_str, yesterday_str, ticker).reset_index()
            if df.empty:
                continue

            df['종목코드'] = ticker
            df['종목명'] = pkstock.get_market_ticker_name(ticker)

            df = df[[
                '날짜',
                '종목코드',
                '종목명',
                'BPS',
                'PER',
                'PBR',
                'EPS',
                'DIV',
                'DPS'
            ]]
            all_data.append(df)
        except Exception as e:
            print(f'[{ticker}] 에러 발생: {e}')

    if not all_data:
        print('수집된 데이터 없음.')
        return df_all

    df_new = pd.concat(all_data, ignore_index=True)
    df_new['날짜'] = pd.to_datetime(df_new['날짜'])

    # 병합 및 저장
    df_all = pd.concat([df_all, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=['날짜', '종목코드'], keep='last', inplace=True)
    df_all.to_parquet(filepath)

    print(f"{yesterday_str}까지 반영 완료. 총 {len(df_all):,} rows → {filepath}")
    return df_all

def update_kospi_marketcap(filepath="kospi_marketcap.parquet"):
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y%m%d')

    # 기존 데이터 로드
    if os.path.exists(filepath):
        df_all = pd.read_parquet(filepath)
        print(f'MarketCap 기존 파일 로드됨: {len(df_all)}: rows')

        latest_date = df_all['날짜'].max().date()
        if latest_date >= yesterday:
            print('이미 최신 데이터까지 포함되어 있음')
            return df_all
        start = latest_date + timedelta(days=1)
    else:
        df_all = pd.DataFrame()
        start = today - timedelta(days=365)

    start_str = start.strftime('%Y%m%d')

    # 종목 리스트
    tickers = pkstock.get_market_ticker_list(market='KOSPI')
    all_data = []

    for ticker in tqdm(tickers, desc='종목별 시가총액 수집'):
        try:
            df = pkstock.get_market_cap_by_date(start_str, yesterday_str, ticker).reset_index()
            if df.empty:
                continue

            df['종목코드'] = ticker
            df['종목명'] = pkstock.get_market_ticker_name(ticker)

            df = df[[
                '날짜',
                '종목코드',
                '종목명',
                '시가총액',
                '거래량',
                '거래대금'
            ]]

            all_data.append(df)
        except Exception as e:
            print(f'[{ticker}] 에러 발생: {e}')

    if not all_data:
        print('수집된 데이터 없음.')
        return df_all

    df_new = pd.concat(all_data, ignore_index=True)
    df_new['날짜'] = pd.to_datetime(df_new['날짜'])

    # 병합 및 저장
    df_all = pd.concat([df_all, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=['날짜', '종목코드'], keep='last', inplace=True)
    df_all.to_parquet(filepath)

    print(f"{yesterday_str}까지 반영 완료. 총 {len(df_all):,} rows → {filepath}")
    return df_all
    
def update_kospi_sector(csv_path='업종분류 현황.csv'):
    df_sector = pd.read_csv(csv_path, encoding='euc-kr')
    df_sector['종목코드'] = df_sector['종목코드'].astype(str).str.zfill(6)
    return df_sector

def update_kospi(clean=True):
    price_df = update_kospi_ohlcv()
    fundamental_df = update_kospi_fundamental()
    marketcap_df = update_kospi_marketcap()
    sector_df = update_kospi_sector()

    merge_df = pd.merge(
        price_df,
        sector_df[['종목코드', '업종명']],
        on=['종목코드'],
        how='left'
    )

    merge_df = pd.merge(
        merge_df,
        fundamental_df.drop(columns=['종목명']),
        on=['날짜', '종목코드'],
        how='left'
    )

    merge_df = pd.merge(
        merge_df,
        marketcap_df.drop(columns=['종목명']),
        on=['날짜', '종목코드'],
        how='left'
    )

    cols = ['날짜', '종목코드', '종목명', '업종명', '종가',
            '등락률', '시가총액', '거래량', '거래대금',
            'BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS']

    result = merge_df[cols]
    if clean is True:
        result = drop_nan(result)
        
    return result

def drop_nan(df, excluded_tickers_path='excluded_tickers.json'):
    with open(excluded_tickers_path) as file:
        excluded_tickers = set(json.load(file)['excluded_tickers'])

    df = df[~df['종목코드'].isin(excluded_tickers)].reset_index(drop=True)
    df['등락률'] = df['등락률'].fillna(0)
    return df

def make_scaled_df(df):
    # 종가: 종목코드별 MinMax 정규화
    df['종가_scaled'] = df.groupby('종목코드')['종가'].transform(
        lambda x: MinMaxScaler().fit_transform(x.values.reshape(-1, 1)).flatten()
    )

    # 시가총액: 업종명별 로그 + MinMax 정규화
    df['시가총액_sector_scaled'] = df.groupby('업종명')['시가총액'].transform(
        lambda x: MinMaxScaler().fit_transform(np.log1p(x.values).reshape(-1, 1)).flatten()
    )

    # 시가총액: 전체 로그 + MinMax 정규화
    df['시가총액_scaled'] = MinMaxScaler().fit_transform(
        np.log1p(df['시가총액']).values.reshape(-1, 1)
    ).flatten()

    # 거래량: 전체 로그 + Robust 정규화
    df['거래량_scaled'] = RobustScaler().fit_transform(
        np.log1p(df['거래량']).values.reshape(-1, 1)
    ).flatten()

    # 거래대금: 종목코드별 로그 + MinMax 정규화
    df['거래대금_scaled'] = df.groupby('종목코드')['거래대금'].transform(
        lambda x: MinMaxScaler().fit_transform(np.log1p(x.values).reshape(-1, 1)).flatten()
    )

    # PER: 업종명별 Robust 정규화
    df['PER_sector_scaled'] = df.groupby('업종명')['PER'].transform(
        lambda x: RobustScaler().fit_transform(x.values.reshape(-1, 1)).flatten()
    )

    # EPS: 업종명별 Robust 정규화
    df['EPS_sector_scaled'] = df.groupby('업종명')['EPS'].transform(
        lambda x: RobustScaler().fit_transform(x.values.reshape(-1, 1)).flatten()
    )

    # PBR: 종목코드별 Robust 정규화
    df['PBR_sector_scaled'] = df.groupby('종목코드')['PBR'].transform(
        lambda x: RobustScaler().fit_transform(x.values.reshape(-1, 1)).flatten()
    )

    # BPS: 종목코드별 Standard 정규화
    df['BPS_sector_scaled'] = df.groupby('종목코드')['BPS'].transform(
        lambda x: StandardScaler().fit_transform(x.values.reshape(-1, 1)).flatten()
    )

    # DIV, DPS: 전체 MinMax 정규화
    df['DIV_scaled'] = MinMaxScaler().fit_transform(df[['DIV']])
    df['DPS_scaled'] = MinMaxScaler().fit_transform(df[['DPS']])

    return df
