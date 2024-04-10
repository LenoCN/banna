import pandas as pd
import numpy as np
from itertools import product
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import pyarrow.parquet as pp
import pyarrow as pa

# 读取数据
file = './data_with_ticket.parquet'
df = pd.read_parquet(file)

# 需处理的列
columns_to_exclude = ['日期', '股票简称', '收益', '昨日涨停情况', '总市值']
columns_to_filter = df.columns.difference(columns_to_exclude)

# 所有表格转化为float类型
df[columns_to_filter] = df[columns_to_filter].apply(pd.to_numeric, errors='coerce')

# 计算筛选列的平均值
averages = df[columns_to_filter].mean()

# 定义计算函数
def calculate_return(combo):
    results = []
    for filter_type in product(['greater', 'less'], repeat=len(combo)):
        for zhangting_condition in ['涨停', '炸板', '不涨停']:
            condition = np.ones(len(df), dtype=bool)
            for col, f_type in zip(combo, filter_type):
                condition &= df[col] > averages[col] if f_type == 'greater' else df[col] < averages[col]
            condition &= df['昨日涨停情况'] == zhangting_condition
            filtered_df = df[condition]
            avg_return = filtered_df['收益'].mean()
            count = filtered_df['收益'].count()
            results.append({
                'Combination': combo,
                'Filter_Type': filter_type,
                'ZhangTing_Condition': zhangting_condition,
                'Average_Return': avg_return,
                'Count': count
            })
    return results

# 并行计算所有组合
with ProcessPoolExecutor() as executor:
    futures = [executor.submit(calculate_return, combo) for combo in product(columns_to_filter, repeat=2)]
    results = [future.result() for future in futures]

# 扁平化结果列表
flattened_results = [item for sublist in results for item in sublist]

# 创建结果DataFrame
results_df = pd.DataFrame(flattened_results)

# 保存结果
time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
pp.write_table(pa.Table.from_pandas(results_df), f'./parquet_data_ana/data_ana_{time_str}.parquet')
results_df.to_csv(f'data_ana_{time_str}.csv', encoding='GBK', index=False)
