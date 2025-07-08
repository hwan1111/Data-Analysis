from sklearn.preprocessing import MinMaxScaler, RobustScaler, StandardScaler

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
