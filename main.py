import os
import json
import asyncio
import aiohttp
import time
import pandas as pd
import sqlalchemy


path_to_data_save_folder = os.path.join('endpoints_data_collection')

clines_columns_descriptions = {
    't_o': 'Kline open time',
    'p_o': 'Open price',
    'p_h': 'High price',
    'p_l': 'Low price',
    'p_c': 'Close price',
    'v': 'Volume',
    't_c': 'Kline Close time',
    'v_q_a': 'Quote asset volume',
    'n': 'Number of trades',
    'v_b': 'Taker buy base asset volume',
    'v_q': 'Taker buy quote asset volume',
    'ig': 'Unused field, ignore.',
}

async def get_cline(symbol, API_URL: str="https://api.binance.com/api/v3/klines",):

    async with aiohttp.ClientSession() as session:

        params = {
            "symbol": f"{symbol}",
            "interval": "1m",
            "limit": 1
        }

        async with session.get(API_URL, params=params) as response:
            data = await response.json()

            df = pd.DataFrame(
                data,
                columns=clines_columns_descriptions.keys()
            )[['t_c', 'p_o',	'p_h',	'p_l',	'p_c', 'v_q_a',	'n', 'v_b',	'v_q',]]

            df['symbol'] = symbol
            
            print(f'{symbol}_cline have been get')

            return df
    
async def get_order_book_snapshot(
    symbol: str,
    limit: int=100,
    API_URL: str='https://api.binance.com/api/v3/depth'
) -> pd.DataFrame:
    
    async with aiohttp.ClientSession() as session:

        snapshot_url = f"{API_URL}?symbol={symbol}&limit={limit}"

        columns = ['bids_price', 'bids_volume', 'asks_price', 'asks_volume']

        async with session.get(snapshot_url) as response:
            data = await response.json()

            df = pd.concat(
                [
                    pd.DataFrame(data[x], columns=[f'{x}_price', f'{x}_volume'])
                    for x in ['bids', 'asks']
                ],
                axis=1
            )

            df['lastUpdateId'] = data['lastUpdateId']

            df['symbol'] = symbol
            
            print(f'{symbol}_order_book_snapshot have been get')

            return df
    
async def get_futures_open_interest(
    symbol: str,
    API_URL: str="https://binance.com/fapi/v1/openInterest"
):   
    async with aiohttp.ClientSession() as session:

        url_futures_open_interest = (
            f'{API_URL}'
            f'?symbol={symbol}'
        )

        async with session.get(url_futures_open_interest) as response:
            data = await response.json()

            df = pd.DataFrame(
                {
                    # multiply open interest for close price
                    'symbol': [data['symbol']],
                    'o_i': [data['openInterest']],
                    't_o_i': [data['time']],
                }
            )
            
            print(f'{symbol}_futures_open_interest have been get')

            return df
        
async def get_top_long_short_ratio(
    symbol: str,
    source: str,
    API_URL: str=f'https://binance.com/futures/data/topLongShort'
):
    async with aiohttp.ClientSession() as session:
        
        url_long_short_ratio_base = (
            API_URL 
            + 
            f'{source}Ratio'
        )

        period = '5m'

        limit = '1'

        url_long_short_ratio = (
            f'{url_long_short_ratio_base}'
            f'?symbol={symbol}'
            f'&period={period}'
            f'&limit={limit}'
        )

        async with session.get(url_long_short_ratio) as response:
            data = await response.json()

            data = data[0]

            if source == 'Account':

                df = pd.DataFrame(
                    {
                        'symbol': [data['symbol']],
                        'l_s_t_a' : [data['longAccount']],
                        't_l_s_t_a' : [data['timestamp']]
                    }
                )
            
            if source == 'Position':

                df = pd.DataFrame(
                    {
                        'symbol': [data['symbol']],
                        'l_s_t_p' : [data['longAccount']],
                        't_l_s_t_p' : [data['timestamp']]
                    }
                )
            
            print(f'{symbol}_{source}_top_long_short_ratio have been get')

            return df

async def get_long_short_ratio_account(
    symbol: str,
    API_URL: str='https://binance.com/futures/data/globalLongShortAccountRatio',
):
    async with aiohttp.ClientSession() as session:

        period = '5m'

        limit = '1'

        url_long_short_ratio = (
            f'{API_URL}'
            f'?symbol={symbol}'
            f'&period={period}'
            f'&limit={limit}'
        )

        async with session.get(url_long_short_ratio) as response:
            data = await response.json()

            data = data[0]

            df = pd.DataFrame(
                {
                    'symbol': [data['symbol']],
                    'l_s_g_a' : [data['longAccount']],
                    't_l_s_g_a' : [data['timestamp']]
                }
            )
            
            print(f'{symbol}_long_short_ratio have been get')
            
            return df

async def get_server_time(API_URL: str="https://api.binance.com/api/v3/time"):
    async with aiohttp.ClientSession() as session:
        async with session.get(API_URL) as response:
            data = await response.json()

            df = pd.DataFrame({'server_time': [data['serverTime']]})
            
            print(f'serverTime have been get')

            return df

async def main(symbols, db_params):

    while True:
        # Calculate the number of seconds until the next minute
        current_time = time.localtime()
        seconds_until_next_minute = 59 - current_time.tm_sec
        if seconds_until_next_minute <= 0:
            seconds_until_next_minute += 60
            
        # Wait until the 59th second of the next minute           
        await asyncio.sleep(seconds_until_next_minute)
        # Run the coroutine at 59th second
        tasks = (
            [get_server_time()]
            +
            [get_cline(symbol) for symbol in symbols]
            +
            [get_order_book_snapshot(symbol) for symbol in symbols]
            +
            [get_futures_open_interest(symbol) for symbol in symbols]
            +
            [get_top_long_short_ratio(symbol, source='Account',) for symbol in symbols]
            +
            [get_top_long_short_ratio(symbol, source='Position',) for symbol in symbols]
            +
            [get_long_short_ratio_account(symbol) for symbol in symbols]
        )

        request_send_time = (time.time_ns() // 1_000_000)

        results = await asyncio.gather(*tasks)

        response_recieve_time = (time.time_ns() // 1_000_000)
        
        # Save results by symbols
        conn = sqlalchemy.create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}/{db_params['database']}"
        )

        n_symbols = len(symbols)
        
        tables_names = [
            'server_time',
            'clines',
            'order_book',
            'open_interests',
            'long_short_ratio_top_accounts',
            'long_short_ratio_top_positions',
            'long_short_ratio_global_accounts',
        ]

        # save the server time table that common for all symbols
        results[0].to_sql(
            tables_names[0],
            conn,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        results = results[1:]      

        for i, table_name in enumerate(tables_names[1:]):
            for j, symbol in enumerate(symbols):      
                results[(n_symbols * i) + j].to_sql(
                    table_name,
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
        
        writing_data_finish_time = (time.time_ns() // 1_000_000)

        print(f'task start time {request_send_time}')
        print(f'task finish time {response_recieve_time}')
        print(f'task during time {(response_recieve_time - request_send_time) // 1000} s')
        print(f'writing data time: {(writing_data_finish_time - response_recieve_time) // 1000} s')


# Example usage
symbols = [
    'BTCUSDT',
    'XRPUSDT',
    'ETHUSDT',
    'USDCUSDT',
    'TRXUSDT',
    'DOGEUSDT',
    'STMXUSDT',
    'LINKUSDT',
    'SOLUSDT',
    # 'LTCUSDT',
    # 'BNBUSDT',
    # 'XLMUSDT',
    # 'WAVESUSDT',
    # 'MATICUSDT',
    # 'MASKUSDT'
]  # List of top 15 symbols end with 'USDT'

# Get credentials
with open('credentials\db_credentials.json') as file:
    credentials = json.load(file)

database = credentials['db_name']# 'your_database_name'
user = credentials['username']# 'your_username'
password = credentials['password']# 'your_password'
host = credentials['endpoint']# 'your_host'
port = credentials['port']

# Create a connection to the database
db_params = {
    'host': host,
    'port': port,
    'user': user,
    'password': password,
    'database': database
}

asyncio.run(main(symbols, db_params))
