import pandas as pd
from datetime import datetime
import pyarrow.parquet as pp
import pyarrow as pa
import sys
import numpy as np
import time

import pandas as pd
import pymannkendall as mk
import matplotlib.pyplot as plt
import math

def filter_and_calculate_and_plot(df_raw, df_conditions):
    num_conditions = len(df_conditions)
    num_cols = math.ceil(math.sqrt(num_conditions))
    num_rows = math.ceil(num_conditions / num_cols)

    fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, num_rows * 5))
    axes = axes.flatten()

    columns_to_exclude = ['日期', '股票简称', '收益', '昨日涨停情况', '总市值']
    columns_to_filter = [col for col in df_raw.columns if col not in columns_to_exclude]

    df_raw[columns_to_filter] = df_raw[columns_to_filter].apply(pd.to_numeric)
    averages = df_raw[columns_to_filter].mean()

    for index, (ax, row) in enumerate(zip(axes, df_conditions.iterrows())):
        _, row = row
        combination = row['Combination']
        filter_type = row['Filter_Type']
        zhangting_condition = row['ZhangTing_Condition']
        conditions = df_raw['昨日涨停情况'] == zhangting_condition
        for col, f_type in zip(combination, filter_type):
            if f_type == 'greater':
                conditions &= (df_raw[col] > averages[col])
            elif f_type == 'less':
                conditions &= (df_raw[col] < averages[col])
        filtered_df = df_raw[conditions]
        daily_mean_return = filtered_df.groupby('日期')['收益'].mean()
        cumulative_return = daily_mean_return.cumprod()

        mk_result = mk.original_test(cumulative_return)

        ax.plot(cumulative_return, label=f'Condition {index + 1}')
        trend_message = f'Trend: {mk_result.trend}, S: {mk_result.s}, p-value: {mk_result.p:.5f}'
        ax.set_title(f'Condition {index + 1}', fontsize=9)
        ax.set_xlabel('Date', fontsize=8)
        ax.set_ylabel('Cumulative Return', fontsize=8)
        ax.legend(fontsize=7)
        ax.text(0.5, -0.2, trend_message, ha="center", transform=ax.transAxes, fontsize=7,
                bbox={"facecolor":"orange", "alpha":0.5, "pad":3})

    plt.tight_layout()
    plt.show()
    

if __name__ == '__main__':
    # 读取原始数据 ： 近4年热度排名500以内的所有股票
    file = './data_clean.parquet'
    df_raw = pd.read_parquet(file)
    # 读取conditions数据
    file = './data_final_20240406_191956.parquet'
    df_conditions = pd.read_parquet(file)
    
    # 筛选Cumulative_Return列最大的20行
    df_conditions = df_conditions.nlargest(20, 'Cumulative_Return')
    df_conditions = df_conditions.reset_index(drop=True)

    # 调用函数并传入数据A和数据B
    cumulative_return_result = filter_and_calculate_and_plot(df_raw, df_conditions)
