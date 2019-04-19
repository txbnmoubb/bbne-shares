#!/usr/bin/env python
#coding=utf-8
'''
Create on 2017/07/27 
@email: 284203271@qq.com
version 1.0
'''
#主程序
import os
import sys
import time
import datetime
import MySQLdb
import Generically
import tushare as ts
import pandas as pd
from pandas import Series, DataFrame
from sqlalchemy import create_engine,VARCHAR
from business_calendar import Calendar, MO, TU, WE, TH, FR
from apscheduler.schedulers.blocking import BlockingScheduler

DatabaseName='st'
engine = create_engine('mysql://st:shares.111@localhost/st?charset=utf8')

connect_stdb= MySQLdb.connect(
        host='localhost',
        port = 3306,
        user='st',
        passwd='shares.111',
        db ='st',
        )
cur = connect_stdb.cursor()


def get_today_all():
    '''获取当天交易数据'''
    Generically.log_recoard_filename('exec','get_today_all is exec.')
    datestr = datetime.datetime.now()
    date=datestr.strftime("%Y%m%d")
    tablename='get_today_all_'+date
    if not Generically.check_table_isexists(DatabaseName,tablename):
        logstr="%s is not exists. get_today_all starting ..." % tablename
        Generically.log_recoard_filename('get_data',logstr)
        try:
            df=ts.get_today_all()
            df.to_sql(tablename, engine, if_exists='replace')
        except:
            Generically.log_recoard_filename('error','get_today_all() error')

def get_data_main_01(FunctionName):
    Generically.log_recoard_filename('exec','%s is exec.' % FunctionName)
    shares=get_all_shares()
    for code in shares.code:
        TableName = "%s_%s" % (FunctionName, code)
        Generically.log_recoard_filename('get_data',"%s start download %s data." % (FunctionName,TableName))
        if Generically.check_table_isexists(DatabaseName,TableName):
            Generically.log_recoard_filename('get_data',"%s:%s is exists." % (TableName,code))
            update_get_data_main_01(FunctionName,code,DatabaseName,TableName)
            continue
        try:
            if FunctionName == 'get_hist_data':
                df = ts.get_hist_data(code,retry_count=10,pause=30)
            elif FunctionName == 'get_h_data':
                Generically.log_recoard_filename('test','start down qfq')
                df2014 = ts.get_h_data(code, autype='qfq', start='2014-01-01', end='2014-12-31', retry_count=10, pause=30)
                df2015 = ts.get_h_data(code, autype='qfq', start='2015-01-01', end='2015-12-31', retry_count=10, pause=30)
                df2016 = ts.get_h_data(code, autype='qfq', start='2016-01-01', end='2016-12-31', retry_count=10, pause=30)
                df2017 = ts.get_h_data(code, autype='qfq', start='2017-01-01', end='2017-12-31', retry_count=10, pause=30)
                Generically.log_recoard_filename('test', 'end down qfq ')
        except:
            Generically.log_recoard_filename('error', '%s() download error' % (FunctionName))
            continue
        if FunctionName == 'get_h_data':
            if df2014 is None or df2015 is None or df2016 is None or df2017 is None:
                Generically.log_recoard_filename('get_data', "%s %s error ." % (code, FunctionName))
                continue
        elif FunctionName == 'get_hist_data':
            if df is None:
                Generically.log_recoard_filename('get_data',"%s %s error ." % (code,FunctionName))
                continue
        try:
            if FunctionName == 'get_h_data':
                df2014.to_sql(TableName, engine, if_exists='replace')
                df2015.to_sql(TableName, engine, if_exists='append')
                df2016.to_sql(TableName, engine, if_exists='append')
                df2017.to_sql(TableName, engine, if_exists='append')
            elif FunctionName == 'get_hist_data':
                df.to_sql(TableName, engine, if_exists='replace', dtype={'date': VARCHAR(df.index.get_level_values('date').str.len().max())})
        except:
            Generically.log_recoard_filename('error', '%s() to_sql error' % (FunctionName))
            continue
        time.sleep(1)

def update_get_data_main_01(FunctionName,code,DatabaseName,TableName):
    print "update_get_history_data_all:",code
    #cal=Calendar()
    datelist=Generically.before_daily_num(10)
    if FunctionName == 'get_h_data':
        table_linedata_exists = Generically.check_date_line_for_get_h_data(TableName, 'date')
    elif FunctionName == 'get_hist_data':
        table_linedata_exists = Generically.check_date_line(TableName, 'date')
    trade_call_percodesisclose = Generically.check_trade_call_percodeisclose(code)
    isclose_list={}
    codes=[]
    dates=[]
    isclose=[]
    for datestr in datelist:
        if isOpenDay[datestr:datestr]['isOpen'][0] == 0:
            continue
        #if not Generically.check_table_linedate(DatabaseName,TableName,'date',datestr):
        if not datestr in table_linedata_exists and datestr not in trade_call_percodesisclose:
            #Generically.log_recoard_filename('get_data','update_history_date:line code %s %s is not exists.' % (code,datestr))
            try:
                if FunctionName == 'get_hist_data':
                    df=ts.get_hist_data(code,datestr,datestr,retry_count=10,pause=30)
                elif FunctionName == 'get_h_data':
                    df = ts.get_h_data(code, autype='qfq', start=datestr, end=datestr, retry_count=10, pause=30)
            except:
                Generically.log_recoard_filename('error', '%s update() error' % FunctionName)
                continue
            print df
            if len(df)==1:
                df.to_sql(TableName, engine, if_exists='append', dtype={'date': VARCHAR(df.index.get_level_values('date').str.len().max())})
                Generically.log_recoard_filename('get_data', "update_%s:line %s %s is update success." % (FunctionName, code, datestr))
            else:
                Generically.log_recoard_filename('get_data','update_%s:line code %s %s is not exists and update error.' % (FunctionName,code,datestr))
                codes.append(code)
                dates.append(datestr)
                isclose.append(5)
        else:
            Generically.log_recoard_filename("get_date", "update_%s:line %s %s line is exists or iscloseday." % (FunctionName, code, datestr))
            continue
        isclose_list = {'code': codes, 'date': dates, 'isclose': isclose}
        df = DataFrame(isclose_list)
        df.to_sql('trade_call_percodesisclose', engine, if_exists='append')
        if len(df) > 1:
            Generically.log_recoard_filename('trade_call_percodesisclose',"trade_call_percodesisclose:%s append." % code)
        return True
        time.sleep(1)


def get_data_main_02(FunctionName):
    Generically.log_recoard_filename('exec','%s is exec.' % FunctionName)
    shares=get_all_shares()
    for code in shares.code:
        Generically.log_recoard_filename('get_data',"%s start download %s data." % (FunctionName,code))
        TableName="%s_%s" % (FunctionName,code)
        if not Generically.check_table_isexists(DatabaseName,TableName):
            Generically.log_recoard_filename('get_data',"%s:%s is not exists." % (TableName,code))
            table_is_exists=False
            #create table first
            beforeOpenDay=Generically.before_open_day()
            Generically.log_recoard_filename('get_data','%s: create table %s %s first' % (FunctionName,code,beforeOpenDay))
            try:
                if FunctionName == 'get_tick_data':
                    df = ts.get_tick_data(code,beforeOpenDay,retry_count=10,pause=30)
            except:
                Generically.log_recoard_filename('error', '%s() download error' % FunctionName)
                continue
            df['date']='0000-00-00'
            try:
                if len(df) == 3: #当获取上一个交易日，不是开盘日的话，跳过。等开盘的时候再下载数据。主要针对tick数据
                    Generically.log_recoard_filename('error', '%s() iscloseday' % FunctionName)
                    continue
                df.to_sql(TableName,engine,if_exists='replace')
            except:
                continue
        update_get_data_main_02(FunctionName,code,DatabaseName,TableName)
        time.sleep(1)

def update_get_data_main_02(FunctionName,code,DatabaseName,TableName):
    print "update_get_tick_data_all:",code
    if FunctionName == 'get_tick_data':
        update_beforeday_num=299
    datelist=Generically.before_daily_num(update_beforeday_num)
    table_linedata_exists=Generically.check_date_line(TableName,'date')
    trade_call_percodesisclose=Generically.check_trade_call_percodeisclose(code)
    isclose_list={}
    codes=[]
    dates=[]
    isclose=[]
    for datestr in datelist:
        if isOpenDay[datestr:datestr]['isOpen'][0] == 0:
            continue
        #if not Generically.check_table_linedate(DatabaseName,TableName,'date',datestr):
        if not datestr in table_linedata_exists and datestr not in trade_call_percodesisclose:
            #Generically.log_recoard_filename('get_data','update_history_date:line code %s %s is not exists.' % (code,datestr))
            try:
                if FunctionName == 'get_tick_data':
                    df=ts.get_tick_data(code,datestr,retry_count=10,pause=3)
            except:
                Generically.log_recoard_filename('error', '%s update() error' % FunctionName)
                continue
            #print df
            if len(df)>5:
                df['date']=datestr
                df.to_sql(TableName, engine, if_exists='append')
                #print df
                Generically.log_recoard_filename('get_data',"update_%s:line %s %s is download success." % (FunctionName,code,datestr))
                time.sleep(0.5)
            elif len(df)==3:
                Generically.log_recoard_filename('get_data',"update_%s:line %s %s is NaN.(当天没有数据)." % (FunctionName,code,datestr))
                codes.append(code)
                dates.append(datestr)
                isclose.append(0)
            else:
                Generically.log_recoard_filename('get_data',"update_%s:line %s %s is download error." % (FunctionName,code,datestr))
                codes.append(code)
                dates.append(datestr)
                isclose.append(2)
        else:
            Generically.log_recoard_filename("get_date", "update_%s:line %s %s line is exists or iscloseday." %  (FunctionName,code,datestr))
            continue
    isclose_list={'code':codes,'date':dates,'isclose':isclose}
    df=DataFrame(isclose_list)
    df.to_sql('trade_call_percodesisclose',engine,if_exists='append')
    if len(df)>1:
        Generically.log_recoard_filename('trade_call_percodesisclose',"trade_call_percodesisclose:%s append." % code)
    return True

def create_trade_call_percodesisclose(DatabaseName):
    '''创建个股是否为交易日表'''
    tablename='trade_call_percodesisclose'
    if not Generically.check_table_isexists(DatabaseName,tablename):
        Generically.log_recoard_filename('trade_call_percodesisclose',"%s is not exists. create it ..." % tablename)
        data = {'code': ['000000'],
                'date': ['0000-00-00'],
                'isclose': [0]}
        df=DataFrame(data)
        df.to_sql(tablename,engine,if_exists='replace')

def get_all_shares():
    sql="SELECT * FROM ShareList WHERE CODE LIKE '000%' AND timeToMarket LIKE '1996%'"
    sql="SELECT * FROM ShareList WHERE CODE IN (002202,000637,600590,600703,600261,600075,600874,600388,600198,002104,002089,000909,000725,600171,002230,000977,002161,000997,000701,002017,600118,002046,600456,002405,600879,000768,600581,600841,600150,601766,600111,600980,002080,600143,002012,000970,600110,600237,002007,002252,000790,002166,600513,000998,000930,600538,600875,601727,002227,002318,600151,002202,000868,000559,600366,002190,000839,000762,002227)"
    df=pd.read_sql(sql,connect_stdb)
    return df

def main():
    get_today_all()  #get today all

if __name__ == '__main__':
    Generically.log_recoard_filename("exec","get shares data main.py is start !")
    #get_today_all()
    isOpenDay=Generically.get_tradecall() #获取最近7年所有交易日列表
    create_trade_call_percodesisclose(DatabaseName) #创建一个空表，用来记录个股的某一天是否为交易日
    ### 废弃 ##get_history_data_all()  # 获取历史日K线数据，过去10天
    ### 废弃 ##get_tick_data_all(FunctionName='get_tick_data') #逐笔交易,过去300天
    get_data_main_01('get_h_data') # 获取历史日K线前复权数据，过去10天
    #get_data_main_01('get_hist_data')  # 获取历史日K线数据，过去10天
    #get_data_main_02('get_tick_data')  # tick数据,过去300天
    #sched = BlockingScheduler()
    #sched.add_job(get_history_data_all, 'interval', seconds=43200, id='my_job1')
    #sched.add_job(get_today_all, 'cron', hour=18, minute=30, id='my_job2')
    #sched.add_job(get_tick_data_all, 'cron', hour=16, minute=30, id='my_job3')
    #sched.start()