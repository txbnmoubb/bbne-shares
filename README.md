# bbne-shares
脚本执行前，需要下面操作

1. 交易日数据，存入原始表中，每年更新一次
df = ts.trade_cal()
df.to_sql('trade_call',engine,if_exists='replace')

2. 初始化股票列表
curdate=(datetime.datetime.now()).strftime("%Y-%m-%d")
df = ts.get_stock_basics()
df['date']=curdate
df.to_sql('ShareList',engine,if_exists='replace')  #忽略报错信息
df.to_sql('ShareList',engine,if_exists='append')

