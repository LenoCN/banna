import pandas as pd
import numpy as np
from itertools import product
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import pyarrow.parquet as pp
import pyarrow as pa
import gc

# 分块读取数据
chunk_size = 100000  # 根据你的内存大小调整
chunks = pd.read_parquet(file, chunksize=chunk_size)

# 初始化结果列表，用于存储每个块的结果
results = []

# 定义计算函数
def calculate_return(chunk, averages, combo):
    # ... 函数内容不变 ...

# 处理每个数据块
for df in chunks:
    # 数据类型转换为 float32
    df[columns_to_filter] = df[columns_to_filter].astype('float32', errors='coerce')
    
    # 计算筛选列的平均值
    averages = df[columns_to_filter].mean()
    
    # 并行计算所有组合
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(calculate_return, df, averages, combo) for combo in product(columns_to_filter, repeat=2)]
        for future in futures:
            # 将结果逐步写入文件或累加到结果列表
            results.extend(future.result())
    
    # 删除不再需要的DataFrame和临时变量以释放内存
    del df
    gc.collect()

# 创建结果DataFrame
results_df = pd.DataFrame(results)

# 保存结果
time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
pp.write_table(pa.Table.from_pandas(results_df), f'./parquet_strategy_mining/strategy_mining_{time_str}.parquet')
results_df.to_csv(f'strategy_mining_{time_str}.csv', encoding='GBK', index=False)
