# bbne-shares
�ű�ִ��ǰ����Ҫ�������

1. ���������ݣ�����ԭʼ���У�ÿ�����һ��
df = ts.trade_cal()
df.to_sql('trade_call',engine,if_exists='replace')

2. ��ʼ����Ʊ�б�
curdate=(datetime.datetime.now()).strftime("%Y-%m-%d")
df = ts.get_stock_basics()
df['date']=curdate
df.to_sql('ShareList',engine,if_exists='replace')  #���Ա�����Ϣ
df.to_sql('ShareList',engine,if_exists='append')

