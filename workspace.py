import pandas as pd
from datetime import datetime
import pyarrow.parquet as pp
import pyarrow as pa
import sys
import numpy as np
import time

def filter_and_calculate(df_raw, df_conditions):
    results = []
    
    # 需处理的列
    columns_to_exclude = ['日期', '股票简称', '收益', '昨日涨停情况', '总市值']
    columns_to_filter = [col for col in df_raw.columns if col not in columns_to_exclude]

    # 所有表格转化为float类型
    df_raw[columns_to_filter] = df_raw[columns_to_filter].apply(pd.to_numeric)
    # 计算筛选列的平均值
    averages = df_raw[columns_to_filter].mean()

    # 遍历数据B中的每一条筛选规则
    for index, row in df_conditions.iterrows():
        if index % 100 == 0:
            print(index, len(df_conditions))
        # 获取当前筛选组合和对应的过滤类型
        combination = row['Combination']
        filter_type = row['Filter_Type']
        zhangting_condition = row['ZhangTing_Condition']
        # 构建筛选条件
        conditions = df_raw['昨日涨停情况'] == zhangting_condition  # 从涨停板条件开始
        for col, f_type in zip(combination, filter_type):
            if f_type == 'greater':
                conditions &= (df_raw[col] > averages[col])  # 使用 &= 应用 AND 条件
            elif f_type == 'less':
                conditions &= (df_raw[col] < averages[col])  # 使用 &= 应用 AND 条件
        # 应用筛选条件到数据A
        filtered_df = df_raw[conditions]
        # 计算同一日期收益的均值
        daily_mean_return = filtered_df.groupby('日期')['收益'].mean()
        # 计算不同日期的收益均值累乘
        cumulative_return = daily_mean_return.prod()
        # 将筛选条件和累乘收益添加到结果列表中
        result = row.to_dict()  # 将当前行转换为字典
        result['Cumulative_Return'] = cumulative_return  # 添加累乘收益
        results.append(result)
    
    # 将结果列表转换为DataFrame
    results_df = pd.DataFrame(results)
    return results_df

if __name__ == '__main__':
    # 读取原始数据 ： 近4年热度排名500以内的所有股票
    file = './data_clean.parquet'
    df_raw = pd.read_parquet(file)
    # 读取conditions数据
    # 第一次 1小时
    #file = './parquet_data_ana/data_ana_20240405_210012.parquet'
    # 第二次 3小时
    file = './parquet_data_ana/data_ana_20240406_003343.parquet'
    df_conditions = pd.read_parquet(file)
    
    # 对conditons进行筛选
    df_conditions = df_conditions[(df_conditions['ZhangTing_Condition'] != '涨停')]
    #df_conditions = df_conditions[(df_conditions['ZhangTing_Condition'] != '涨停') &
                              #(df_conditions['Average_Return'] ** df_conditions['Count'] > 100)] &
                              #(df_conditions['Count'] < 10000) &
                              # (df_conditions['Average_Return'] > 0.95)]
    df_conditions = df_conditions.reset_index(drop=True)


    # 调用函数并传入数据A和数据B
    cumulative_return_result = filter_and_calculate(df_raw, df_conditions)

    # 多格式数据保存
    now = datetime.now()
    time_str = now.strftime("%Y%m%d_%H%M%S")
    cumulative_return_result.to_csv('data_final_'+time_str+'.csv', encoding='GBK', index=False)
    cumulative_return_result.to_csv('data_final_'+time_str+'', index=False)
    pp.write_table(pa.Table.from_pandas(cumulative_return_result), './data_final_'+time_str+'.parquet')

