import pandas as pd
from pytdx_lib import get_first_n_data_elements, is_holiday
import sys
import time
import pyarrow.parquet as pp
import pyarrow as pa
import glob
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pp
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def data_to_df(data):
    # 提取列表和日期
    list_of_ordered_dicts, date = data
    # 将OrderedDict转换为普通字典，然后创建一个包含所有字典的列表
    list_of_dicts = [dict(item) for item in list_of_ordered_dicts]
    # 创建DataFrame
    df = pd.DataFrame(list_of_dicts)
    return df

# 定义一个函数来获取并处理每个(stock_id, date)组合的数据
def get_and_process_data(stock_id, date):
    data = get_first_n_data_elements(stock_id=stock_id, num=21, date=date)
    if data is None:
        return None
    df = data_to_df(data)
    df['date'] = date
    df['stock_id'] = stock_id
    return df

def save_to_parquet(dataframes, counter):
    # 合并DataFrame
    chunk_df = pd.concat(dataframes, ignore_index=True)
    
    # 保存到parquet文件
    now = datetime.now()
    time_str = now.strftime("%Y%m%d_%H%M%S")
    chunk_df.to_csv(f'./parquet_ticket/ticket_chunk_{counter}_{time_str}', index=False)
    pp.write_table(pa.Table.from_pandas(chunk_df), f'./parquet_ticket/ticket_chunk_{counter}_{time_str}.parquet')

def get_ticket(df):
    # 提取股票代码中的数字和日期，然后将每对值作为一个列表存入一个大列表中
    combined_list = [[code[:-3], int(date.replace('年', '').replace('月', '').replace('日', ''))] for code, date in zip(df['股票代码'], df['日期'])]
    combined_list = [item for item in combined_list if str(item[0]).startswith(('00', '30', '60', '68'))]
    # 过滤掉日期是法定节假日的子列表
    combined_list = [
        stock for stock in combined_list 
        if not is_holiday(datetime.strptime(str(stock[1]), "%Y%m%d"))
    ]
    print(len(combined_list))

    b = time.time()
    print(b-a)

    # 假设combined_list是已经定义好的列表
    # 假设get_and_process_data是已经定义好的函数

    chunk_size = 5000  # 设置每个分段的大小
    chunk_counter = 0  # 初始化分段计数器

    combined_list = combined_list[chunk_counter*5000:]

    # 初始化一个空列表用于存储结果
    all_data_list = []

    # 使用ThreadPoolExecutor来并发执行get_and_process_data函数
    with ThreadPoolExecutor(max_workers=50) as executor:
        # 提交任务到线程池并立即返回Future对象列表
        future_to_combination = {executor.submit(get_and_process_data, stock_id, date): (stock_id, date) for stock_id, date in combined_list}

        i=chunk_counter*5000
        # 使用as_completed方法等待线程完成并获取结果
        for future in as_completed(future_to_combination):
            result = future.result()
            if result is not None:
                # 将结果追加到all_data_list中
                all_data_list.append(result)

            # 检查是否收集了足够的结果进行保存
            if len(all_data_list) >= chunk_size:
                save_to_parquet(all_data_list, chunk_counter)
                all_data_list = []  # 重置列表以便收集新的结果
                chunk_counter += 1  # 更新分段计数器

            print(i, len(combined_list))
            i = i + 1

    # 处理剩余的结果（如果有的话）
    if all_data_list:
        save_to_parquet(all_data_list, chunk_counter)

    b = time.time()
    print(b-a)

def ticket_merge(df_raw, df_ticket):
    # 先转换df_raw的股票代码和日期到stock_id和date
    df_raw['stock_id'] = df_raw['股票代码'].str[:-3]
    df_raw['date'] = df_raw['日期'].str.replace('年', '').str.replace('月', '').str.replace('日', '').astype(int)
    # 合并df_raw和df_ticket，根据股票代码和日期
    df_merged = pd.merge(df_raw, df_ticket, on=['stock_id', 'date'], how='left', indicator=True)
    return df_merged

def find_unmarked_rows(df_raw, df_ticket):
    # 先转换df_raw的股票代码和日期到stock_id和date
    df_raw['stock_id'] = df_raw['股票代码'].str[:-3]
    df_raw['date'] = df_raw['日期'].str.replace('年', '').str.replace('月', '').str.replace('日', '').astype(int)
    # 合并df_raw和df_ticket，根据股票代码和日期
    df_merged = pd.merge(df_raw, df_ticket, on=['stock_id', 'date'], how='left', indicator=True)
    # 找出在df_ticket中没有匹配的行
    unmarked_rows = df_merged[df_merged['_merge'] == 'left_only']
    # 选择未标注的行的原始股票代码和日期
    unmarked_stock_dates = unmarked_rows[['股票代码', '日期']]
    return unmarked_stock_dates


if __name__ == '__main__':
    a = time.time()
    # 读取原始行情数据
    file_list = glob.glob('./data_20_24/*.parquet')
    df_raw = pd.concat((pd.read_parquet(file) for file in file_list), ignore_index=True)
    # 去除重复项
    df_raw = df_raw.drop_duplicates()
    
    b = time.time()
    print(b-a)
    
    # 第一步 首次获取ticket
    #get_ticket(df_raw)
    
    # 第二步 
    # 读取ticket行情数据
    while True:
        file_list = glob.glob('./parquet_ticket/ticket_chunk*.parquet')
        df_ticket = pd.concat((pd.read_parquet(file) for file in file_list), ignore_index=True)
        df_ticket = df_ticket.drop_duplicates()
        # 找出没有被成功标注的行
        unmarked_stock_dates = find_unmarked_rows(df_raw, df_ticket)
        # 排除掉非法的日期与股票
        combined_list = [[code[:-3], int(date.replace('年', '').replace('月', '').replace('日', ''))] for code, date in zip(unmarked_stock_dates['股票代码'], unmarked_stock_dates['日期'])]
        combined_list = [item for item in combined_list if str(item[0]).startswith(('00', '30', '60', '68'))]
        combined_list = [
            stock for stock in combined_list 
            if not is_holiday(datetime.strptime(str(stock[1]), "%Y%m%d"))
        ]
        # 再次获取ticket
        if len(combined_list) != 0:
            get_ticket(unmarked_stock_dates)
        else:
            break
    
    # 第三步 多格式数据保存
    now = datetime.now()
    time_str = now.strftime("%Y%m%d_%H%M%S")
    pp.write_table(pa.Table.from_pandas(df_ticket), f'./parquet_ticket/ticket_all_{time_str}.parquet')
    df_ticket.to_csv('ticket',sep='\t')
    