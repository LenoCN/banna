import sys
from datetime import datetime, timedelta
import pandas as pd
import swifter
import pyarrow.parquet as pp
import pyarrow as pa
import glob
from ticket_signal import get_ticket_and_check, calculate_factors, calculate_factors_optimized
from find_leading import format_if_decimal
from get_usdcnh import get_usdcnh_macd
import numpy as np

# 设置显示所有列
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def rename_columns(df):
    rename_mapping = {
        '日期': '日期',
        '股票简称': '股票简称',
        '股票代码': '股票代码',
        '总市值': '总市值',
        'a股市值(不含限售股)': '流通值',
        '个股热度排名': '昨日热度排名',
        '曾涨停': '曾涨停',
        '首次涨停时间': '首次涨停时间',
        '分时涨跌幅:前复权': '今日竞价涨幅',
        '分时换手率': '今日31分换手率',
        '分时换手率_3': '昨日31分换手率',
        '分时换手率_1': '今日竞价换手率',
        '分时换手率_2': '昨日竞价换手率',
        '{(}开盘价:不复权{/}分时收盘价:不复权{)}': '收益'
    }
    df.rename(columns=rename_mapping, inplace=True)
    keep_columns = list(rename_mapping.values())
    # 删除除了这些列以外的所有列
    df = df[keep_columns]
    return df

# 判断涨停情况
def judge_zhangting(row):
    if row['首次涨停时间'] != 'nan':
        if row['曾涨停'] == '曾涨停':
            return '炸板'
        elif row['曾涨停'] != '曾涨停':
            return '涨停'
    else:
        return '不涨停'

if __name__ == '__main__':
    # 读取数据
    file_list = glob.glob('./data_20_24/*.parquet')
    df = pd.concat((pd.read_parquet(file) for file in file_list), ignore_index=True)
    # 去除重复项
    df = df.drop_duplicates()
    
    # 重命名加强可读性
    df = rename_columns(df=df)
    # 将除了某些列之外的所有列转换为数值类型
    columns_to_convert = df.columns.difference(['日期', '股票简称', '股票代码', '曾涨停', '首次涨停时间'])
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
    # 插入'竞价换手增幅' '31分换手增幅' '日内增幅' 列
    df['竞价换手增幅'] = df.apply(lambda row: 1 if row['昨日竞价换手率'] == 0 else row['今日竞价换手率'] / row['昨日竞价换手率'], axis=1)
    df['31分换手增幅'] = df.apply(lambda row: 1 if row['昨日31分换手率'] == 0 else row['今日31分换手率'] / row['昨日31分换手率'], axis=1)
    df['日内增幅'] = df.apply(lambda row: 1 if row['今日竞价换手率'] == 0 else row['今日31分换手率'] / row['今日竞价换手率'], axis=1)
    # 浮点小数保留两位 
    df = df.map(format_if_decimal)
    # 判断昨日涨停情况
    df['昨日涨停情况'] = df.apply(judge_zhangting, axis=1)
    df = df.drop(['曾涨停', '首次涨停时间'], axis=1)
    # 添加汇率趋势
    df_usdcnh = get_usdcnh_macd()
    df = df.merge(df_usdcnh[['日期', 'MACD.MACD']], on='日期', how='left')
 
    # 移除包含NaN的行
    df.replace('nan', np.nan, inplace=True)
    df.dropna(inplace=True)
    
    # 计算收益平均值与方差
    column_name = '收益'
    df[column_name] = df[column_name].astype(float)
    mean_value = df[column_name].mean()
    variance_value = df[column_name].var()
    print(f"平均值: {mean_value}, 方差: {variance_value}")
    
    # 保存清洗后数据
    now = datetime.now()
    time_str = now.strftime("%Y%m%d_%H%M%S")
    try:
        df.to_csv('data_clean_'+time_str+'.csv', encoding='GBK', index=False)
    except:
        pass
    try:
        df.to_csv('data_clean_'+time_str,sep='\t')
    except:
        pass
    pp.write_table(pa.Table.from_pandas(df), 'data_clean.parquet')
    pp.write_table(pa.Table.from_pandas(df), './parquet_data_clean/data_clean_'+time_str+'.parquet')
     
    # 获取ticket数据
    ticket_path = './parquet_ticket'
    df_ticket = get_ticket_and_check(df_raw=df, ticket_path=ticket_path)
    # 生成ticket信号并反标回原始行情
    df['formatted_date'] = df['日期'].str.replace('年', '').str.replace('月', '').str.replace('日', '').astype(int)
    df['stock_id'] = df['股票代码'].str.slice(0, -3)
    # 使用 swifter 库并行应用函数
    print('ENtry')
    df[['开盘6秒增益', '开盘6秒方向']] = df.swifter.apply(lambda row: calculate_factors_optimized(row, df_ticket), axis=1)
    #df[['开盘6秒增益', '开盘6秒方向']] = df.apply(lambda row: calculate_factors(row, df_ticket), axis=1)
    
    # 保存完整数据集
    now = datetime.now()
    time_str = now.strftime("%Y%m%d_%H%M%S")
    try:
        df.to_csv('data_with_ticket_'+time_str+'.csv', encoding='GBK', index=False)
    except:
        pass
    try:
        df.to_csv('data_with_ticket'+time_str,sep='\t')
    except:
        pass
    pp.write_table(pa.Table.from_pandas(df), 'data_with_ticket.parquet')
    pp.write_table(pa.Table.from_pandas(df), './parquet_data_with_ticket/data_with_ticket_'+time_str+'.parquet')
 
