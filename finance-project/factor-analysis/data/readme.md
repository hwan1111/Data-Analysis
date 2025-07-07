# 📂 data/

이 디렉토리는 프로젝트에서 사용되는 원시 데이터 또는 전처리된 데이터를 저장하는 공간입니다.

## 디렉토리 구성 안내

| 파일명 | 설명 |
|--------|------|
| `ohlcv_kospi.parquet` | 종목별 일별 시세 (OHLCV) |
| `fundamental_kospi.parquet` | PER/PBR/ROE 등 재무지표 데이터 |
| `marketcap_kospi.parquet` | KOSPI 시가총액 데이터 (update_kospi_marketcap 함수로 생성) |
| `excluded_tickers.json`| 리츠, 스팩(SPAC), 지분증권 등 일반 상장 기업과 다른 회계 구조를 가지거나, 재무지표가 공시되지 않은 비표준 종목의 티커 모음|



## 파일 생성 방법

이 파일들은 모두 `utils.py` 내의 함수 실행을 통해 자동 생성됩니다:

```python
from utils import update_kospi_marketcap
update_kospi_marketcap()
