#coding=utf-8
'''
Create on 2017/07/27 
@email: 284203271@qq.com
'''
import os
import datetime
import MySQLdb
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql://st:shares.111@localhost/st?charset=utf8')

connect_stdb= MySQLdb.connect(
        host='localhost',
        port = 3306,
        user='st',
        passwd='shares.111',
        db ='st',
        )
cur = connect_stdb.cursor()

def check_trade_call_percodeisclose(code):
    datelist=[]
    sql="SELECT DISTINCT(date) FROM trade_call_percodesisclose where code='%s'" % code
    df=pd.read_sql(sql,engine,index_col='date')
    for i in df.index:  datelist.append(i)
    return datelist

def check_date_line(tablename,field_date_name):
    #tablename="get_tick_data_603999"
    datelist=[]
    sql="SELECT DISTINCT(%s) FROM %s" % (field_date_name,tablename)
    df=pd.read_sql(sql,engine,index_col='date')
    for i in df.index:  datelist.append(i)
    return datelist

def check_date_line_for_get_h_data(tablename,field_date_name):
    #tablename="get_tick_data_603999"
    datelist=[]
    #sql="SELECT DISTINCT(%s) FROM %s" % (field_date_name,tablename)
    sql="SELECT DATE_FORMAT(date,'%s') AS date FROM %s" % ('%%Y-%%m-%%d',tablename) #使用一个%报错
    #print sql,'==='
    df=pd.read_sql(sql,engine,index_col='date')
    #print '===--',df,'===--'
    for i in df.index:  datelist.append(i)
    return datelist

def before_open_day():
    isOpenDay=get_tradecall()
    datelist=before_daily_num(10)
    for datestr in datelist:
        if isOpenDay[datestr:datestr]['isOpen'][0] == 1:
            return datestr

def get_tradecall():
    '''获取时间段内的交易日列表'''
    sql="select * from trade_call where calendarDate BETWEEN '2010-01-01' AND '2017-12-31'"
    df=pd.read_sql(sql,engine,index_col='calendarDate')
    data=pd.DataFrame(df)
    return data

def check_db():
    cur.execute("show databases")
    for db in cur.fetchall():
        db_list.append(db[0])
    return db_list

def check_table(db):
    #cur.execute("use %s" % db)
    #cur.execute("select databases")
    #print "current databases: %s" %cur.fetchall()[0]
    tb_list=[]
    all_table = cur.execute("show tables")
    for tb in cur.fetchall():
        tb_list.append(tb[0])
    return tb_list

def exists_tablelist(tablename):
    tb_list=[]
    all_table = cur.execute("show tables")
    for tb in cur.fetchall():
        tb_list.append(tb[0])
    return tb_list

def check_table_isexists(dbname,tablename):
    tb_list=[]
    all_table = cur.execute("show tables")
    for tb in cur.fetchall():
        tb_list.append(tb[0])
    if tablename in tb_list:
    	return True
    else:
    	return False

def check_table_linedate(dbname,tablename,field_date_name,datestr):
    sql="select %s from %s where %s='%s'" % (field_date_name,tablename,field_date_name,datestr)
    cur.execute(sql)
    #print 'check_table_linedate:',tablename,sql
    linenum=len(cur.fetchall())
    if linenum > 0:
        return True
    else:
        return False

def before_daily_num(beforedaynum):
    datelist=[]
    for num in range(1,beforedaynum):
        dateline=(datetime.datetime.now() + datetime.timedelta(days = -num)).strftime("%Y-%m-%d")
        datelist.append(dateline)
    return datelist

def log_recoard_filename(logname,datas):
    tt = datetime.datetime.now()
    logpath = os.path.split(os.path.realpath(__file__))[0]  + "/logs"
    if not os.path.exists(logpath):
        os.makedirs(logpath)

    log_file = open(logpath + "/" + logname + "_" + tt.strftime("%Y%m%d")  + ".log", 'a')
    log_line = tt.strftime("%Y/%m/%d %H:%M:%S") +  ' ' + str(datas) + "\n"
    log_file.writelines(log_line)
    log_file.close()
    print log_line