import pandas as pd
import requests
from fake_useragent import UserAgent
from sqlalchemy import create_engine


def extractor(url):
    try:
        ua = UserAgent()
        user_agent = ua.random

        header = {
            'User-Agent': user_agent
        }

        resp = requests.get(url, headers=header)
        data = resp.json()
        return data
    except Exception as e:
        return f'Fail: {str(e)}'


def transformer(data):
    try:
        df = pd.json_normalize(data, 'data')
        df.drop(columns='maxSupply', inplace=True)
        df.dropna(subset='explorer', ignore_index=True, inplace=True)
        return df
    except Exception as e:
        return f'Fail: {str(e)}'


def loader(df):
    try:
        engine = create_engine('mysql+mysqlconnector://mysql:mysql@localhost/crypto_currency')
        df.to_sql(name='Crypto', con=engine, if_exists='replace', index=False)
        return 'Success'
    except Exception as e:
        return f'Fail: {str(e)}'


def main(url):
    extract_data = extractor(url)
    trans_data = transformer(extract_data)
    load_data = loader(trans_data)
    print(load_data)


if __name__ == '__main__':
    main('http://api.coincap.io/v2/assets')
