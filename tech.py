#!/usr/bin/env python
# coding: utf-8

# In[1]:

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import datetime

import backtrader as bt
import talib 
from futu import *
import os
import datetime
import empyrical as ey
from collections import OrderedDict



# In[3]:


#用 backtrader_plotting 的 bokeh 绘图

from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo


# In[259]:


start_date = '2018-10-01'
end_date ='2020-09-20'
stock_list =['HK.00700','HK.01810','HK.01211','HK.03690']


# In[ ]:





# In[219]:


## 聚宽北上、 南下 交易量数据

from jqdatasdk import *
auth('18688709107','chan123456')

table = finance.STK_ML_QUOTA
df_hk  = finance.run_query(query(
        table.day, table.quota_daily, table.quota_daily_balance
    ).filter(
        table.link_id.in_(['310003', '310004']), table.day<='2020-09-20'  #沪股通、深股通
    ).order_by(table.day))
df_cn  = finance.run_query(query(
        table.day, table.quota_daily, table.quota_daily_balance
    ).filter(
        table.link_id.in_(['310001', '310002']), table.day<='2020-09-20'  #沪股通、深股通
    ).order_by(table.day))
df_cn.to_csv('to_cn.csv')
df_hk.to_csv('to_hk.csv')


# In[225]:


df_hk=pd.read_csv('to_hk.csv',index_col=0)
df_cn=pd.read_csv('to_cn.csv',index_col=0)


# In[260]:


# 从富途获取股票数据

def get_stockdata(stock_list,start_date,end_date):
    df={}
    for stock in stock_list:
        
        quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
        ret, stock_data, page_req_key = quote_ctx.request_history_kline(stock, start=start_date, end=end_date)  
        quote_ctx.close() 
        
        
        stock_data['date']=pd.to_datetime(stock_data['time_key'])
        stock_data=stock_data.set_index('date',drop=True)
        stock_data=stock_data.drop(columns=['time_key'])
        
        stock_data['openinterest']=0
        df.update({stock:stock_data})
    return df
    
   
    


# In[261]:


df_stock=get_stockdata(stock_list,start_date,end_date)


# In[264]:


##自建交易类

class TXStrategy(bt.Strategy):
    # 设置简单均线周期，以备后面调用

    params = (
        ('myperiod', 60),
    )

    def log(self, txt, dt=None):
        # 日记记录输出
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 初始化数据参数
        self.date = self.datas[0].datetime.date(0).isoformat() 
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.signal_1='no'
        self.buylist =[]
        self.boll_window = 120
        self.boll_signaltag= None
        self.lastlist =[]
        self.daily_volume = df_cn
        self.stocklist = stock_list
        self.df_stock = df_stock

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 如果有订单提交或者已经接受的订单，返回退出
            return
            # 主要是检查有没有成交的订单，如果有则日志记录输出价格，金额，手续费。注意，如果资金不足是不会成交订单的
        
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED ,Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  
                self.log(
                    'SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))
            
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:  
            return
        self.log(' OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))
         
    def get_boll(self,date):
        """
        获取北向资金布林带    
        
        净买入额在布林线下轨以下时清仓
        净买入额在布林线上轨以上时执行调仓
        """        
        stdev_n = 2
        
        money_df = self.daily_volume
               
        if len(money_df[money_df.day==date]) <1 :
                        
            return 'None'
        
        idx=money_df[money_df['day']==date].index
        idx=idx[-1]
        money_df=money_df.loc[:idx].copy()
        money_df['net_amount'] = money_df['quota_daily'] - money_df['quota_daily_balance'] #每日额度-每日剩余额度=净买入额
        # 分组求和
        money_df = money_df.groupby('day')[['net_amount']].sum().iloc[-self.params.myperiod:] #过去self.params.myperio天求和
        mid = money_df['net_amount'].mean()
        stdev = money_df['net_amount'].std()
        upper = mid + stdev_n * stdev
        lower = mid - stdev_n * stdev
        mf = money_df['net_amount'].iloc[-1]
        
        if mf >=int(upper):

            self.boll_signaltag ='UPCROSS'
            return 'up'
                        
        # 净买入额在布林线下轨以下时清仓
        elif  mf <= lower:

            self.boll_signaltag ='DOWNCROSS'
            return 'down'
        else:
            return 'no signal'
                
    def get_daily_list(self):
        
        MA=13
        i=0 # 计数器初始化
        # 创建保持计算结果的DataFrame
        df = pd.DataFrame()
        date = self.datas[0].datetime.date(0).isoformat()
        for security in self.stocklist:
            # 获取股票的收盘价            
            close_data = self.df_stock[security]            
            # 获取股票现价
            idx= close_data.index.get_loc(date)
            if idx < MA:                
                return df
            close_data = close_data.iloc[idx - MA+1 :idx+1]['close']            
            current_price = close_data.iloc[-1]                                   
            cp_increase = (current_price/close_data[0]-1)*100            
            # 取得平均价格
            ma_n1 = close_data.mean()
            # 计算前一收盘价与均值差值    
            pre_price = (current_price/ma_n1-1)*100            
            df.loc[i,'股票代码'] = security # 把标的股票代码添加到DataFrame
            df.loc[i,'股票名称'] = 'tx' # 把标的股票名称添加到DataFrame
            df.loc[i,'周期涨幅'] = cp_increase # 把计算结果添加到DataFrame
            df.loc[i,'均线差值'] = pre_price # 把计算结果添加到DataFrame
            i=i+1                     
        df = df.fillna(-100)
        df.sort_values(by='周期涨幅',ascending=False,inplace=True) # 按照涨幅排序        
        df.reset_index(drop=True, inplace=True) # 重新设置索   
        for t in df.index:
            if df.loc[t,'周期涨幅'] < 0.1 or df.loc[t,'均线差值'] < 0:
                df=df.drop(t)
        
        return df['股票代码'].tolist()[0:2]
                   
    def next(self):
        date = self.datas[0].datetime.date(0).isoformat()
        tradesignal = self.get_boll(date)
       
        self.buylist = self.get_daily_list()

        self.log('*****************************************************************************')
        self.lastlist=[]
        self.log('账户总值 %2f' % self.broker.get_value())
        for i, d in enumerate(self.datas):
            pos = self.getposition(d)            
            if len(pos):
                self.lastlist.append(str(d._name))             
                print('{}, 持仓:{}, 成本价:{}, 当前价:{}, 盈亏:{:.2f}'.format(
                    d._name, pos.size, pos.price, pos.adjbase, pos.size * (pos.adjbase - pos.price)))             
              
        print('self.buylist: ' ,self.buylist)
        print('self.lastlist：' ,self.lastlist) 
        
        if tradesignal == 'down':
            self.log("交易信号: 空仓")   
            for data in self.datas:
                self.close(data)    
            self.lastlist=[]
            
        elif tradesignal == 'up':
            self.log("交易信号:买入")
            ratio = len(self.buylist)            
            daily_value = self.broker.get_value()
            self.log('daily_value %.2f' % daily_value)
            
            for to_sell in self.lastlist:                
                if to_sell not in self.buylist:                    
                    self.close(to_sell)
                    self.log('调仓卖出 %s' %to_sell)
                    
            for to_buy in self.buylist:
                if to_buy  not in self.lastlist:
                    buycash = daily_value/ratio
                    self.order_target_value(to_buy,buycash*0.9)            
                    self.log('调仓买入 %s' %to_buy)
            
        elif self.boll_signaltag =='UPCROSS':
            self.log("交易信号:boll中值以上，持有 ")
            ratio = len(self.buylist)
            daily_value = self.broker.get_value()                      
            
            if len(self.buylist) < 1:
                for d in self.lastlist:
                    self.close(d)
                self.lastlist=[]
   
            if set(self.lastlist)==set(self.buylist):
                return
            else :
                for to_sell in self.lastlist:                
                    if to_sell not in self.buylist:
                        self.close(to_sell)
                        self.log('卖出 %s' %to_sell)

                for to_buy in self.buylist:
                        buycash = daily_value/ratio
                        self.order_target_value(to_buy,buycash*0.9)
                        self.log('调仓买入 %s' %to_buy)
        else:
            self.log("交易信号:boll中值以下，空仓 ")
                                                  
'''    def settingCerebro(self):
        # 添加回撤观察器
        self.__cerebro.addobserver(bt.observers.DrawDown)
        # 设置手续费
        self.__cerebro.broker.setcommission(commission=self.__commission)
        # 设置初始资金为0.01
        self.__cerebro.broker.setcash(self.__initcash)
        # 添加分析对象
        self.__cerebro.addanalyzer(btay.SharpeRatio, _name = "sharpe", riskfreerate = 0.02)
        self.__cerebro.addanalyzer(btay.AnnualReturn, _name = "AR")
        self.__cerebro.addanalyzer(btay.DrawDown, _name = "DD")
        self.__cerebro.addanalyzer(btay.Returns, _name = "RE")
        self.__cerebro.addanalyzer(btay.TradeAnalyzer, _name = "TA")                        

    # 获取回测指标
    def getResult(self):
        return self.__backtestResult
'''


# In[267]:


cerebro = bt.Cerebro()

cerebro.addstrategy(TXStrategy)

#data = bt.feeds.PandasData(dataname=TX_data,name='HK.00700',fromdate=datetime.datetime(2019, 12, 1),todate=datetime.datetime(2020, 8, 31))
#data2 = bt.feeds.PandasData(dataname=df_stock['HK.09988'],name='HK.09988',fromdate=datetime.datetime(2019, 12, 1),todate=datetime.datetime(2020, 8, 31))
    
for s in stock_list:
    feed = bt.feeds.PandasData(dataname =df_stock[s] ,fromdate=datetime.datetime(2018, 10, 1),todate=datetime.datetime(2020,9, 20))
    cerebro.adddata(feed, name = s)

cerebro.broker.setcash(1000000.0)
    # 设置每笔交易交易的股票数量
cerebro.addsizer(bt.sizers.FixedSize, stake=100)
    # 设置手续费
cerebro.broker.setcommission(commission=0.002)

    # 输出初始资金
print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # 运行策略

    # 输出结果
    
#cerebro.optstrategy(TXStrategy,myperiod=range(100, 140,10))    

'''
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')
cerebro.addanalyzer(bt.analyzers.Calmar, _name='_Calmar')
cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
# cerebro.addanalyzer(bt.analyzers.TimeDrawDown, _name='_TimeDrawDown')
cerebro.addanalyzer(bt.analyzers.GrossLeverage, _name='_GrossLeverage')
cerebro.addanalyzer(bt.analyzers.PositionsValue, _name='_PositionsValue')
cerebro.addanalyzer(bt.analyzers.LogReturnsRolling, _name='_LogReturnsRolling')
cerebro.addanalyzer(bt.analyzers.PeriodStats, _name='_PeriodStats')
cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns')
cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')
# cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='_SharpeRatio_A')
cerebro.addanalyzer(bt.analyzers.SQN, _name='_SQN')
cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')
cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='_TradeAnalyzer')
cerebro.addanalyzer(bt.analyzers.Transactions, _name='_Transactions')
cerebro.addanalyzer(bt.analyzers.VWR, _name='_VWR')
#cerebro.addanalyzer(bt.analyzers.TotalValue, _name='_TotalValue')
#cerebro.addanalyzer(bt.analyzers.PositionsValue, _name='_TotalValue')
#cerebro.addobserver(bt.observers.CashValue)
'''
cerebro.run()

print('Close Portfolio Value: %.2f' % cerebro.broker.getvalue())

#run_cerebro_plot(cerebro)
b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
cerebro.plot(b)

cerebro.plot()
#get_ipython().run_line_magic('matplotlib', 'inline')


# In[177]:


# 判断仓位方法，用无序


#set(t1) - set(t2)


# In[ ]:




