import numpy as np
import pandas as pd
from util import logger, dwh


def get_df_supermarket():
    N_SALES_TREND = 125
    N_STD = 50
    N_SMOOTH = 125
    N_TOPSELL = 50
    N_DEMAND = 25

    # Lấy dữ liệu sản phẩm bán hàng từ siêu thị
    df_sales = dwh.query('''
    SELECT productid, saledate, revenue
    FROM supermarket.stg_sales_product
    WHERE revenue > 0
    ORDER BY productid, saledate
    ''')
    df_sales = df_sales.astype({'saledate': 'datetime64'})

    # Tính số sản phẩm có doanh thu cao hơn MA 20 ngày
    df_uptrend = (
        df_sales
        .assign(prod_ma=lambda df: df.groupby('productid')['revenue'].rolling(window=20, closed='left').mean().values)
        .dropna()
        .assign(uptrend=lambda df: df['revenue'] > df['prod_ma'])
        .pivot_table(index='saledate', values='uptrend', aggfunc='sum').reset_index()
    )

    # Chỉ số sản phẩm bán chạy trong 52 tuần
    df_topsell = (
        df_sales
        .assign(max_rev=lambda df: df.groupby('productid')['revenue'].rolling(window=250, closed='left').max().values)
        .assign(min_rev=lambda df: df.groupby('productid')['revenue'].rolling(window=250, closed='left').min().values)
        .eval("high = revenue / max_rev - 1")
        .eval("low = revenue / min_rev - 1")
        .pivot_table(index='saledate', values='productid', aggfunc='count').reset_index()
        .rename(columns={'productid': 'num_products_sold'})
        .eval("topsell_index = num_products_sold.rolling(@N_TOPSELL).mean()")
    )

    # Breadth: số sản phẩm tăng/giảm doanh thu trong ngày
    df_demand = (
        df_sales
        .assign(diff=lambda df: df.groupby('productid')['revenue'].diff())
        .dropna()
        .assign(flag=lambda df: np.select(
            condlist=[df['diff'] > 0, df['diff'] < 0],
            choicelist=[1, -1],
            default=0
        ))
        .pivot_table(index='saledate', columns='flag', values='productid', aggfunc='count').reset_index()
        .rename(columns={1: 'increased', -1: 'decreased', 0: 'stable'})
        .fillna(0)
        .eval("net_demand = increased - decreased")
        .eval("demand_index = net_demand.rolling(@N_DEMAND).mean()")
    )

    # Dữ liệu tổng doanh thu siêu thị
    df_market = dwh.query('''
    SELECT saledate, marketvalue
    FROM supermarket.stg_market_summary
    WHERE saledate >= '2015-01-01'
    ORDER BY saledate
    ''')
    df_market = df_market.astype({'saledate': 'datetime64'})

    def assign_rsi(df):
        diff = df['marketvalue'].diff()
        up = np.where(diff > 0, diff, 0)
        down = np.where(diff < 0, -diff, 0)
        gain = pd.Series(up).ewm(com=13, min_periods=14).mean()
        loss = pd.Series(down).ewm(com=13, min_periods=14).mean()
        df['rsi'] = 100 - 100 / (1 + gain / loss)
        return df

    df_final = (
        df_market
        .eval("momentum = marketvalue / marketvalue.rolling(@N_SALES_TREND).mean() - 1")
        .eval("volatility = marketvalue.pct_change().rolling(@N_STD).std()")
        .eval("volatility = volatility / volatility.rolling(@N_SMOOTH).mean() - 1")
        .pipe(assign_rsi)
        .merge(df_uptrend, on='saledate', how='left')
        .merge(df_topsell, on='saledate', how='left')
        .merge(df_demand, on='saledate', how='left')
        .eval("ratio_uptrend = uptrend / num_products_sold")
    )

    components = ['momentum', 'volatility', 'ratio_uptrend', 'demand_index', 'topsell_index']
    for col in components:
        df_final[col] = df_final[col].rolling(window=500).rank(pct=True).round(2) * 100

    df_final['supermarket_health_score'] = (df_final[components].mean(axis=1) / 100).round(2) * 100
    df_final['createddatetime'] = pd.Timestamp.now()
    return df_final[['saledate', 'supermarket_health_score', 'marketvalue', 'rsi', 'createddatetime']]
