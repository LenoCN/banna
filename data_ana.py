import pandas as pd
import itertools
import sys
import time
import numpy as np
import pyarrow.parquet as pp
import pyarrow as pa
from datetime import datetime

start_time = time.time()

# 读取数据
file = './data_clean.parquet'
df = pd.read_parquet(file)

# 需处理的列
columns_to_exclude = ['日期', '股票简称', '收益', '昨日涨停情况', '总市值']
columns_to_filter = [col for col in df.columns if col not in columns_to_exclude]

# 所有表格转化为float类型
df[columns_to_filter] = df[columns_to_filter].apply(pd.to_numeric)

# 计算筛选列的平均值
averages = df[columns_to_filter].mean()

# 找到所有可能的列组合
combinations = list(itertools.chain.from_iterable(itertools.combinations(columns_to_filter, L) for L in range(1, len(columns_to_filter)+1)))

# 计算所有可能的组合数量
num_combinations = sum(1 for _ in itertools.chain.from_iterable(itertools.combinations(columns_to_filter, L) for L in range(1, len(columns_to_filter)+1)))
# 计算每个组合的筛选类型的数量，这是2的组合长度次方
num_filter_types_per_combination = [2**len(combo) for combo in itertools.combinations(columns_to_filter, 1)]
# 计算总循环次数
total_loops = num_combinations * sum(num_filter_types_per_combination)
print(f"Total number of loops: {total_loops}")

results = []

id = 0
# 遍历所有组合并计算收益平均值
for combo in combinations:
    #id = id + 1
    #if(id == 10):
    #    break
    # 计算并打印运行时间
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(combo)
    print(f"Program took {elapsed_time} seconds to run.")
    for filter_type in itertools.product(['greater', 'less'], repeat=len(combo)):
        for zhangting_condition in ['涨停', '炸板', '不涨停']:
            # 创建筛选条件的组合
            condition = np.ones(len(df), dtype=bool)
            for col, f_type in zip(combo, filter_type):
                if f_type == 'greater':
                    condition &= df[col] > averages[col]
                else:
                    condition &= df[col] < averages[col]
            
            # 应用昨日涨停的筛选条件
            condition &= df['昨日涨停情况'] == zhangting_condition
            
            # 应用筛选条件
            filtered_df = df[condition]

            # 计算筛选后的收益平均值、数量和累乘结果
            avg_return = filtered_df['收益'].mean()
            count = filtered_df['收益'].count()
            #print(combo, zhangting_condition, filter_type, avg_return , count)

            # 将结果添加到临时结果列表中
            results.append({
                'Combination': combo,
                'Filter_Type': filter_type,
                'ZhangTing_Condition': zhangting_condition,
                'Average_Return': avg_return,
                'Count': count
            })

# 创建结果DataFrame
results_df = pd.DataFrame(results)

# 计算并打印运行时间
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Program took {elapsed_time} seconds to run.")

# 多格式数据保存
now = datetime.now()
time_str = now.strftime("%Y%m%d_%H%M%S")
pp.write_table(pa.Table.from_pandas(results_df), './parquet_data_ana/data_ana_'+time_str+'.parquet')
results_df.to_csv('data_ana.csv', encoding='GBK', index=False)
results_df.to_csv('data_ana',sep='\t')