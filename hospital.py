import numpy as np
import pandas as pd
from util import logger, dwh


def get_df_hospital():
    N_TREND = 125
    N_STD = 50
    N_SMOOTH = 125
    N_SUCCESS = 50
    N_BREADTH = 25

    # Lấy dữ liệu bệnh nhân điều trị
    df_patient = dwh.query('''
    SELECT patientid, treatmentdate, treatmentcost
    FROM hospital.stg_patient_treatment
    WHERE treatmentcost > 0
    ORDER BY patientid, treatmentdate
    ''')
    df_patient = df_patient.astype({'treatmentdate': 'datetime64'})

    # Số ca điều trị có chi phí cao hơn MA20
    df_uptrend = (
        df_patient
        .assign(cost_ma=lambda df: df.groupby('patientid')['treatmentcost'].rolling(window=20, closed='left').mean().values)
        .dropna()
        .assign(uptrend=lambda df: df['treatmentcost'] > df['cost_ma'])
        .pivot_table(index='treatmentdate', values='uptrend', aggfunc='sum').reset_index()
    )

    # Chỉ số thành công trong 52 tuần
    df_success = (
        df_patient
        .assign(max_cost=lambda df: df.groupby('patientid')['treatmentcost'].rolling(window=250, closed='left').max().values)
        .assign(min_cost=lambda df: df.groupby('patientid')['treatmentcost'].rolling(window=250, closed='left').min().values)
        .eval("high = treatmentcost / max_cost - 1")
        .eval("low = treatmentcost / min_cost - 1")
        .pivot_table(index='treatmentdate', values='patientid', aggfunc='count').reset_index()
        .rename(columns={'patientid': 'num_patients'})
        .eval("success_index = num_patients.rolling(@N_SUCCESS).mean()")
    )

    # Breadth: số bệnh nhân chi phí tăng/giảm so với ca trước
    df_breadth = (
        df_patient
        .assign(diff=lambda df: df.groupby('patientid')['treatmentcost'].diff())
        .dropna()
        .assign(flag=lambda df: np.select(
            condlist=[df['diff'] > 0, df['diff'] < 0],
            choicelist=[1, -1],
            default=0
        ))
        .pivot_table(index='treatmentdate', columns='flag', values='patientid', aggfunc='count').reset_index()
        .rename(columns={1: 'increased', -1: 'decreased', 0: 'stable'})
        .fillna(0)
        .eval("net = increased - decreased")
        .eval("breadth_index = net.rolling(@N_BREADTH).mean()")
    )

    # Dữ liệu tổng chi phí bệnh viện
    df_hospital = dwh.query('''
    SELECT treatmentdate, totalcost
    FROM hospital.stg_hospital_summary
    WHERE treatmentdate >= '2015-01-01'
    ORDER BY treatmentdate
    ''')
    df_hospital = df_hospital.astype({'treatmentdate': 'datetime64'})

    def assign_rsi(df):
        diff = df['totalcost'].diff()
        up = np.where(diff > 0, diff, 0)
        down = np.where(diff < 0, -diff, 0)
        gain = pd.Series(up).ewm(com=13, min_periods=14).mean()
        loss = pd.Series(down).ewm(com=13, min_periods=14).mean()
        df['rsi'] = 100 - 100 / (1 + gain / loss)
        return df

    df_final = (
        df_hospital
        .eval("momentum = totalcost / totalcost.rolling(@N_TREND).mean() - 1")
        .eval("volatility = totalcost.pct_change().rolling(@N_STD).std()")
        .eval("volatility = volatility / volatility.rolling(@N_SMOOTH).mean() - 1")
        .pipe(assign_rsi)
        .merge(df_uptrend, on='treatmentdate', how='left')
        .merge(df_success, on='treatmentdate', how='left')
        .merge(df_breadth, on='treatmentdate', how='left')
        .eval("ratio_uptrend = uptrend / num_patients")
    )

    components = ['momentum', 'volatility', 'ratio_uptrend', 'breadth_index', 'success_index']
    for col in components:
        df_final[col] = df_final[col].rolling(window=500).rank(pct=True).round(2) * 100

    df_final['hospital_health_score'] = (df_final[components].mean(axis=1) / 100).round(2) * 100
    df_final['createddatetime'] = pd.Timestamp.now()
    return df_final[['treatmentdate', 'hospital_health_score', 'totalcost', 'rsi', 'createddatetime']]
