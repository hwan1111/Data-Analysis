import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm

from pykrx import stock as pkstock
from pykrx import bond as pkbond

from src.data.cleaner import drop_nan

def update_kospi_ohlcv(filepath="data/kospi_ohlcv.parquet"):
    today = datetime.today().date()
    today_str = today.strftime('%Y%m%d')
    
    # 어제를 기준으로 가장 가까운 개장일을 찾는다
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y%m%d')
    nearest_opening_date_str = pkstock.get_nearest_business_day_in_a_week(yesterday_str, prev=True)
    nearest_opening_date = datetime.strptime(nearest_opening_date_str, "%Y%m%d").date()

    # 기존 데이터 로드
    if os.path.exists(filepath):
        df_all = pd.read_parquet(filepath)
        print(f"OHLCV 기존 파일 로드됨: {len(df_all):,} rows")

        latest_date = df_all['날짜'].max().date()
        if latest_date >= nearest_opening_date:
            print(f"{nearest_opening_date}까지 이미 반영되어 있어, 갱신 불필요.")
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
            df = pkstock.get_market_ohlcv_by_date(start_str, nearest_opening_date_str, ticker).reset_index()
            if df.empty:
                continue
            df['종목코드'] = ticker
            df['종목명'] = pkstock.get_market_ticker_name(ticker)
            df = df[['날짜', '종목코드', '종목명', '종가']]
            all_data.append(df)
        except Exception as e:
            print(f"[{ticker}] 에러 발생: {e}")

    if not all_data:
        print("수집된 데이터 없음.")
        return df_all

    df_new = pd.concat(all_data, ignore_index=True)
    df_new['날짜'] = pd.to_datetime(df_new['날짜'])

    df_all = pd.concat([df_all, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=['날짜', '종목코드'], keep='last', inplace=True)

    df_all.sort_values(['종목코드', '날짜'], inplace=True)
    df_all['등락률'] = df_all.groupby('종목코드')['종가'].pct_change() * 100
    df_all.sort_index(inplace=True)

    df_all.to_parquet(filepath)
    print(f"{nearest_opening_date_str}까지 반영 완료. 총 {len(df_all):,} rows → {filepath}")

    return df_all

def update_kospi_fundamental(
    filepath='data/kospi_fundamental.parquet',
    json_path='data/excluded_tickers.json'):

    # 제외할 종목 코드 로딩
    with open(json_path, 'r') as file:
        EXCLUDED_TICKERS = set(json.load(file)['excluded_tickers'])

    # 오늘 날짜
    today = datetime.today().date()
    today_str = today.strftime('%Y%m%d')

    # 어제를 기준으로 가장 가까운 개장일을 찾는다
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y%m%d')
    nearest_opening_date_str = pkstock.get_nearest_business_day_in_a_week(yesterday_str, prev=True)
    nearest_opening_date = datetime.strptime(nearest_opening_date_str, "%Y%m%d").date()

    # 기존 데이터 로드
    if os.path.exists(filepath):
        df_all = pd.read_parquet(filepath)
        print(f'Fundamental 기존 파일 로드됨: {len(df_all):,} rows')

        latest_date = df_all['날짜'].max().date()
        if latest_date >= nearest_opening_date:
            print(f"{nearest_opening_date}까지 이미 반영되어 있어, 갱신 불필요.")
            return df_all
        start = latest_date + timedelta(days=1)
    else:
        df_all = pd.DataFrame()
        start = today - timedelta(days=365)
    
    start_str = start.strftime('%Y%m%d')

    # 종목 리스트
    tickers = pkstock.get_market_ticker_list(market='KOSPI')
    all_data = []

    for ticker in tqdm(tickers, desc='종목별 재무재표 수집'):
        try:
            if ticker in EXCLUDED_TICKERS:
                continue
            
            df = pkstock.get_market_fundamental_by_date(start_str, nearest_opening_date_str, ticker).reset_index()
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

    print(f"{nearest_opening_date_str}까지 반영 완료. 총 {len(df_all):,} rows → {filepath}")
    return df_all

def update_kospi_marketcap(filepath="data/kospi_marketcap.parquet"):
    today = datetime.today().date()
    today_str = today.strftime('%Y%m%d')
    
    # 어제를 기준으로 가장 가까운 개장일을 찾는다
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y%m%d')
    nearest_opening_date_str = pkstock.get_nearest_business_day_in_a_week(yesterday_str, prev=True)
    nearest_opening_date = datetime.strptime(nearest_opening_date_str, "%Y%m%d").date()

    # 기존 데이터 로드
    if os.path.exists(filepath):
        df_all = pd.read_parquet(filepath)
        print(f'MarketCap 기존 파일 로드됨: {len(df_all):,} rows')

        latest_date = df_all['날짜'].max().date()
        if latest_date >= nearest_opening_date:
            print(f"{nearest_opening_date}까지 이미 반영되어 있어, 갱신 불필요.")
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
            df = pkstock.get_market_cap_by_date(start_str, nearest_opening_date_str, ticker).reset_index()
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

    print(f"{nearest_opening_date_str}까지 반영 완료. 총 {len(df_all):,} rows → {filepath}")
    return df_all
    
def update_kospi_sector(csv_path='data/업종분류 현황.csv'):
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

    merge_df.sort_values(['종목명', '날짜'], inplace=True)

    result = merge_df[cols]
    if clean is True:
        result = drop_nan(result)
        
    return result
