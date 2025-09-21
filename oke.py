import numpy as np
import pandas as pd
from util import logger, dwh


def get_hospital_metrics():
    WINDOW_TREND = 125
    WINDOW_STD = 50
    WINDOW_SMOOTH = 125
    WINDOW_SUCCESS = 50
    WINDOW_BREADTH = 25

    # ----------------------------
    # Lấy dữ liệu bệnh nhân điều trị
    # ----------------------------
    df_treatments = dwh.query('''
        SELECT patient_id, treatment_date, treatment_cost
        FROM hospital.fact_patient_treatment
        WHERE treatment_cost > 0
        ORDER BY patient_id, treatment_date
    ''')
    df_treatments = df_treatments.astype({'treatment_date': 'datetime64'})

    # ----------------------------
    # Uptrend: số ca chi phí cao hơn MA20
    # ----------------------------
    df_trend = (
        df_treatments
        .assign(cost_ma=lambda df: df.groupby('patient_id')['treatment_cost']
                                       .rolling(window=20, closed='left')
                                       .mean().values)
        .dropna()
        .assign(is_uptrend=lambda df: df['treatment_cost'] > df['cost_ma'])
        .pivot_table(index='treatment_date', values='is_uptrend', aggfunc='sum')
        .reset_index()
    )

    # ----------------------------
    # Success index: chỉ số thành công trong 250 ngày (~52 tuần)
    # ----------------------------
    df_success = (
        df_treatments
        .assign(max_cost=lambda df: df.groupby('patient_id')['treatment_cost']
                                      .rolling(window=250, closed='left')
                                      .max().values)
        .assign(min_cost=lambda df: df.groupby('patient_id')['treatment_cost']
                                      .rolling(window=250, closed='left')
                                      .min().values)
        .eval("high_ratio = treatment_cost / max_cost - 1")
        .eval("low_ratio = treatment_cost / min_cost - 1")
        .pivot_table(index='treatment_date', values='patient_id', aggfunc='count')
        .reset_index()
        .rename(columns={'patient_id': 'num_patients'})
        .eval("success_index = num_patients.rolling(@WINDOW_SUCCESS).mean()")
    )

    # ----------------------------
    # Breadth: số bệnh nhân chi phí tăng/giảm
    # ----------------------------
    df_breadth = (
        df_treatments
        .assign(cost_diff=lambda df: df.groupby('patient_id')['treatment_cost'].diff())
        .dropna()
        .assign(change_flag=lambda df: np.select(
            condlist=[df['cost_diff'] > 0, df['cost_diff'] < 0],
            choicelist=[1, -1],
            default=0
        ))
        .pivot_table(index='treatment_date', columns='change_flag', values='patient_id', aggfunc='count')
        .reset_index()
        .rename(columns={1: 'num_increased', -1: 'num_decreased', 0: 'num_stable'})
        .fillna(0)
        .eval("net_change = num_increased - num_decreased")
        .eval("breadth_index = net_change.rolling(@WINDOW_BREADTH).mean()")
    )

    # ----------------------------
    # Dữ liệu tổng chi phí bệnh viện
    # ----------------------------
    df_hospital = dwh.query('''
        SELECT treatment_date, total_cost
        FROM hospital.fact_hospital_summary
        WHERE treatment_date >= '2015-01-01'
        ORDER BY treatment_date
    ''')
    df_hospital = df_hospital.astype({'treatment_date': 'datetime64'})

    # ----------------------------
    # Hàm tính RSI
    # ----------------------------
    def compute_rsi(df):
        diff = df['total_cost'].diff()
        gain = np.where(diff > 0, diff, 0)
        loss = np.where(diff < 0, -diff, 0)
        avg_gain = pd.Series(gain).ewm(com=13, min_periods=14).mean()
        avg_loss = pd.Series(loss).ewm(com=13, min_periods=14).mean()
        df['rsi'] = 100 - 100 / (1 + avg_gain / avg_loss)
        return df

    # ----------------------------
    # Tích hợp tất cả chỉ số
    # ----------------------------
    df_final = (
        df_hospital
        .eval("momentum = total_cost / total_cost.rolling(@WINDOW_TREND).mean() - 1")
        .eval("volatility = total_cost.pct_change().rolling(@WINDOW_STD).std()")
        .eval("volatility = volatility / volatility.rolling(@WINDOW_SMOOTH).mean() - 1")
        .pipe(compute_rsi)
        .merge(df_trend, on='treatment_date', how='left')
        .merge(df_success, on='treatment_date', how='left')
        .merge(df_breadth, on='treatment_date', how='left')
        .eval("ratio_uptrend = is_uptrend / num_patients")
    )

    # ----------------------------
    # Chuẩn hóa các thành phần và tính điểm tổng hợp
    # ----------------------------
    score_components = ['momentum', 'volatility', 'ratio_uptrend', 'breadth_index', 'success_index']
    for col in score_components:
        df_final[col] = df_final[col].rolling(window=500).rank(pct=True).round(2) * 100

    df_final['hospital_health_score'] = (df_final[score_components].mean(axis=1) / 100).round(2) * 100
    df_final['created_datetime'] = pd.Timestamp.now()

    return df_final[['treatment_date', 'hospital_health_score', 'total_cost', 'rsi', 'created_datetime']]
