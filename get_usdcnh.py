import pandas as pd

def get_usdcnh_macd():
    file_path = r'./USDCNH.txt'
    encoding = 'gbk'
    # Read the text file using the specified separator and encoding
    df = pd.read_csv(file_path, sep=r'\s+', encoding=encoding, skiprows=2)
    # 移除最后一行
    df = df.iloc[:-1]
    # Convert the '时间' column to the desired date format
    df['时间'] = pd.to_datetime(df['时间']).dt.strftime('%Y年%m月%d日')
    df.rename(columns={'时间': '日期'}, inplace=True)
    
    return df


if __name__ == '__main__':
    df = get_usdcnh_macd()
    print(df)