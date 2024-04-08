import pywencai
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pp
import pyarrow as pa
import re
import mult_thread as mt
import sys
import time

# 设置显示所有列
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#cookie = 'PHPSESSID=c0bd5999ef3f9b01ef50feb05bef8c2f; cid=56c69e7f80c5f63783213274fdcc5b0d1672839513; ComputerID=56c69e7f80c5f63783213274fdcc5b0d1672839513; WafStatus=0; wencai_pc_version=1; ta_random_userid=i3nuahvoik; other_uid=Ths_iwencai_Xuangu_capmda2bumoytyx77kawlhhz0scee3mh; THSSESSID=6f13bb6a0c50de810f4a71ba4f; u_ukey=A10702B8689642C6BE607730E11E6E4A; u_uver=1.0.0; u_dpass=A2gMe4KE4gKxbT%2FzqtQY5cxwL3EX7wRCv%2FVLhpeZo0O4hNL0x9Tas8y9%2FKT3cE3e%2FsBAGfA5tlbuzYBqqcUNFA%3D%3D; u_did=C15C4C7A4B464560AA5C2ECCECC1B3DD; u_ttype=WEB; user=MDpteF81MTAzMjU4MTU6Ok5vbmU6NTAwOjUyMDMyNTgxNTo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoxNjo6OjUxMDMyNTgxNToxNzExMzc5MTczOjo6MTU4MTg0MjY0MDo2MDQ4MDA6MDoxY2Q2Mzc4YjM3MDFlZDEyYTAxODg2MjhkZjhjNmE3Njk6ZGVmYXVsdF80OjA%3D; userid=510325815; u_name=mx_510325815; escapename=mx_510325815; ticket=3d4f2bd355bff1dca0a7ef1a816a3304; user_status=0; utk=db8d0c980a42fe1a3d099d2ba742c281; v=A77OzvzYVGGhC4BFf04EjaBtD98F_4KzFMM2XWjDKoH8C1BB0I_SieRThks7'
cookie = 'PHPSESSID=c0bd5999ef3f9b01ef50feb05bef8c2f; cid=56c69e7f80c5f63783213274fdcc5b0d1672839513; ComputerID=56c69e7f80c5f63783213274fdcc5b0d1672839513; WafStatus=0; wencai_pc_version=1; ta_random_userid=i3nuahvoik; other_uid=Ths_iwencai_Xuangu_capmda2bumoytyx77kawlhhz0scee3mh; THSSESSID=6f13bb6a0c50de810f4a71ba4f; u_ukey=A10702B8689642C6BE607730E11E6E4A; u_uver=1.0.0; u_dpass=A2gMe4KE4gKxbT%2FzqtQY5cxwL3EX7wRCv%2FVLhpeZo0O4hNL0x9Tas8y9%2FKT3cE3e%2FsBAGfA5tlbuzYBqqcUNFA%3D%3D; u_did=C15C4C7A4B464560AA5C2ECCECC1B3DD; u_ttype=WEB; ttype=WEB; user=MDpteF81MTAzMjU4MTU6Ok5vbmU6NTAwOjUyMDMyNTgxNTo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoxNjo6OjUxMDMyNTgxNToxNzExOTg0OTI5Ojo6MTU4MTg0MjY0MDo1NzQ2NzE6MDoxYjhiMTM1ZTNmMjNmOTdjYzE5YWFjZDA4ZWY1OTAxMzY6ZGVmYXVsdF80OjA%3D; userid=510325815; u_name=mx_510325815; escapename=mx_510325815; ticket=1ddca5afc4cfbcb18a5c12afce368743; user_status=0; utk=561c15a6c3a54d4fafac817ea09d6a15; v=A04-HoyIpLOZgxCXeTW0PbBdny8VzxIZJJPGrXiWutEM2-CR4F9i2fQjFp1L'
# 自定义函数，用于格式化数字，保留两位小数
def format_if_decimal(value):
    # 检查value是否为None或者不是一个可以转换为float的类型
    if value is None or not isinstance(value, (str, int, float)):
        return value
    try:
        # 尝试将字符串转换为浮点数
        float_value = float(value)
        # 如果转换成功，则格式化为两位小数的字符串
        return '{:.4f}'.format(float_value)
    except ValueError:
        # 如果转换失败，说明不是一个小数，返回原始字符串
        return value

def workdays_list(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    day_count = (end - start).days + 1
    workdays = [
        #(start + timedelta(days=i)).strftime("%m月%d日")
        (start + timedelta(days=i)).strftime("%Y年%m月%d日")
        for i in range(day_count)
        if (start + timedelta(days=i)).weekday() < 5
    ]
    return workdays

def get_res(query):
    return pywencai.get(query=query, sort_key='[1]/[2]', sort_order='asc', cookie = cookie, pro=True, loop=True)


def find_leading(start_date, end_date, sleep_time):
    # 计算开盘日 ： TODO
    workdays = workdays_list(start_date, end_date)
    print(workdays)
    if len(workdays) < 3:
        print('时间区间小于3天无法执行', len(workdays))
        return
    else:
        print('共计天数：', len(workdays))
    
    # 初始化一个空的DataFrame来存储汇总结果
    all_data = pd.DataFrame()

    # 多线程启动
    res_l = []
    thread_l = []
    for i in range(len(workdays) - 2):
        time.sleep(sleep_time)
        query = common_condition_gen(workdays[i], workdays[i+1], workdays[i+2])
        thread = mt.thread_with_date(func=get_res,args=[query], date=workdays[i+1])
        thread_l.append(thread)
        thread_l[-1].start()
        
    # 多线程返回数据收集
    for thread in thread_l:
        thread.join()
        res = thread.get_result()
        if res is None:
            print('get None Type return' + thread.get_date())
            continue
        if isinstance(res, dict):
            print('get dict Type return')
            print(res)
            continue
        # 假设res是查询返回的DataFrame，我们添加一个日期列
        res['日期'] = thread.get_date()
        res.columns = [re.sub(r'\[\d{8}( \d{2}:\d{2})?\]', '', col) for col in res.columns]
        # 使用正则表达式移除表头中的日期
        # 去除重复列名
        columns = res.columns
        new_columns = []
        seen = {}
        for col in columns:
            if col in seen:
                seen[col] += 1
                new_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_columns.append(col)
        res.columns = new_columns
        # 将当前的res追加到汇总的DataFrame中
        if res is None:
            print('Error: No object.' + thread.get_date())
        else:
            res_l.append(res)
        
    # 返回数据聚合
    if res_l == []:
        print('Error: No object.')
        sys.exit()
    res_l = [df.reset_index(drop=True) for df in res_l]
    all_data = pd.concat(res_l, ignore_index=True)
    all_data = all_data.reset_index(drop=True)
    # 浮点小数保留两位 
    all_data = all_data.map(format_if_decimal)
    # 多格式数据保存
    now = datetime.now()
    time_str = now.strftime("%Y%m%d_%H%M%S")
    #try:
    #    df.to_csv('output_'+time_str+'.csv', encoding='GBK', index=False)
    #except:
    #    pass
    #try:
    #    df.to_csv('output_'+time_str,sep='\t')
    #except:
    #    pass
    pp.write_table(pa.Table.from_pandas(all_data), 'find_leading.parquet')
    pp.write_table(pa.Table.from_pandas(all_data), './parquet_find_leading/find_leading_'+time_str+'.parquet')
    return all_data

# 通用条件
def common_condition_gen(last_day, today, tomorrow):
    query = (
        # 股性
        #f"{today}9点31分换手率>1.5;"
        f"{today}9点31分换手率>0;"
        f"{today}总市值;"
        # 情绪
        f"{last_day}热度排名>0;"
        f"{last_day}热度排名<=500;"
        # 筹码结构
        #f"{last_day}不涨停;" TODO
        f"{last_day}曾涨停或{last_day}曾涨停取反或{last_day}不涨停;"
        # 量
        #f"{today}竞价换手率/{last_day}竞价换手率>1.1;"
        #f"{today}9点31分换手率/{last_day}9点31分换手率>1.1;"
        f"{today}竞价换手率;"
        f"{last_day}竞价换手率;"
        f"{today}9点31分换手率;"
        f"{last_day}9点31分换手率;"
        # 趋势
        #f"{last_day}60分钟DEA>0;"
        # 价
        f"{today}09点25分分时涨跌幅;"
        #f"{today}9点31分收盘价/{last_day}收盘价>1'"
        #f"{today}9点31分收盘价/{today}9点25分收盘价>1;"
        # 收益
        f"{tomorrow}开盘价/{today}9点31分时收盘价"
        f"{tomorrow}开盘价/{today}9点31分时收盘价"
    )
    #print(query)
    return query

if __name__ == '__main__':
    #start_date_str = "2024-04-01"
    #end_date_str = "2024-04-03"
    start_date_str = "2020-07-10"
    end_date_str = "2024-04-01"
    #end_date = "2023-11-10"
    
    # 将起止日期之间分为每30天的一个小段
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    current_date = start_date
    date_segments = []
    while current_date < end_date:
        next_date = current_date + timedelta(days=30)
        if next_date > end_date:
            next_date = end_date
        date_segments.append((current_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
        current_date = next_date
    # 每一小段时间单独获取数据
    for segment in date_segments:
        print(segment[0], segment[1])
        sleep_time = 10
        try:
            df = find_leading(start_date=segment[0], end_date=segment[1], sleep_time=sleep_time)
        except:
            print('segment error!')
            print(segment)
            
    
    