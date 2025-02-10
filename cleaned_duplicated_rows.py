import pandas as pd

def cleaned_duplicated_rows(df):
    # 중복된 인덱스 찾기
    duplicated_data = df[df.index.duplicated(keep=False)]

    # 1. 중복된 인덱스에서 결측치가 아닌 값이 있는 경우 -> 우선 선택
    df_no_nan = duplicated_data.dropna(how='all')

    # 2. 중복된 인덱스에서 모든 값이 결측치라면 첫 번째 값만 남김
    df_all_nan = duplicated_data[duplicated_data.isnull().all(axis=1)]
    df_all_nan = df_all_nan.groupby(df_all_nan.index).first()

    # 3. 기존 데이터에서 중복이 없는 행만 유지
    df_cleaned = df.loc[~df.index.duplicated(keep=False)]

    # 4. 결측치 없는 데이터 + 중복 제거된 결측치 데이터 합치기
    df_cleaned = pd.concat([df_cleaned, df_no_nan, df_all_nan]).sort_index()

    # 5. 강제적으로 중복 제거
    df_cleaned = df_cleaned[~df_cleaned.index.duplicated(keep='first')]

    # 최종적으로 중복이 제거되었는지 확인 (0이 나와야 성공)
    is_cleaned = df_cleaned.index.duplicated().sum() == 0
    
    return df_cleaned if is_cleaned else None
