
# 科技股轮动 macd 

import backtrader as bt
import backtrader.indicators as bi
import backtest
import pandas as pd
import math
import matplotlib.pyplot as plt
import datetime
import os
from futu import OpenQuoteContext
import talib as ta
from numpy import array

import sys   
sys.setrecursionlimit(100000)

class Tech_roll(bt.Strategy):
    params =dict(
            myperiod = 120,
            printlog = False,
            setroll = 1,
            setboll = 0)
    
    
    def __init__(self):
        self.order = None
        self.buyprice = 0
        self.comm = 0
        self.buy_size = 0
        self.buy_count = 0
        self.printlog = True

        # 自定义的指标
        self.df_stock=df_stock
        #self.daily_volume=df_cn
        self.stocklist = code
        self.boll_signaltag = None
        self.HSI_data =df_HSI
        

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
        if self.p.setroll == 0:
            return self.stocklist
        else:
            return df['股票代码'].tolist()[0:2]


    def get_boll(self,date):
        """
        获取北向资金布林带    
        
        净买入额在布林线下轨以下时清仓
        净买入额在布林线上轨以上时执行调仓
        """        
        if self.p.setboll ==0:
            return 'up'
        
        stdev_n = 2
        money_df = self.daily_volume
               
        if len(money_df[money_df.day==date]) <1 :
                        
            return 'None'
        
        idx=money_df[money_df['day']==date].index
        idx=idx[-1]
        money_df=money_df.loc[:idx].copy()
        money_df['net_amount'] = money_df['quota_daily'] - money_df['quota_daily_balance'] #每日额度-每日剩余额度=净买入额
        # 分组求和
        money_df = money_df.groupby('day')[['net_amount']].sum().iloc[-self.p.myperiod:] #过去self.params.myperio天求和
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

    def get_macdSignal(self):
        date = self.datas[0].datetime.date(0).isoformat()
        HSI_data = self.HSI_data
        idx = HSI_data.index.get_loc(date)
        pdata=self.HSI_data[idx-60+1:idx+1]
        idxc=pdata['close']
        price = array(idxc)
        macd = ta.MACD(price, fastperiod=5, slowperiod=15, signalperiod=7)[-1]

        if sum(macd[-20:-5])>0:
            if macd[-1]>0:
                return 0
            elif sum(macd[-5:])>0:
                return 1
        return 2
        
        


    def next(self):
        if self.order:
            return
        date = self.datas[0].datetime.date(0).isoformat()
        signal = self.get_macdSignal()
        print (date, ' ------',signal)
        
        
        if signal == 1:
            for stock in self.datas: 
                print(stock._name)
                self.close(stock)
        if signal != 0:
            return

        
        cash=self.broker.get_value()/len(self.stocklist)
            
        for stock in self.datas:
                            
            pos = self.getposition(stock)
            if len(pos)>0:
                return
            else:    
                self.order_target_value(stock,cash*0.9)


        



    # 输出交易记录
    def log(self, txt, dt = None, doprint = False):
        if self.printlog :
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))
            
    def notify_order(self, order):
        # 有交易提交/被接受，啥也不做
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 交易完成，报告结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    '执行买入, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))
                self.buyprice = order.executed.price
                self.comm += order.executed.comm
            else:
                self.log(
                    '执行卖出, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))
                self.comm += order.executed.comm
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("交易失败")
        self.order = None
        
    # 输出手续费
    def stop(self):
        self.log("手续费:%.2f 成本比例:%.5f" % (self.comm, self.comm/self.broker.getvalue()))
    

def get_stockdata(stock_list,start_date,end_date):
    df={}
    for stock in stock_list:
        code = stock
        filename = code+".csv"
        path = "./stock/"        
        if not os.path.exists(path):
            os.makedirs(path)
        if os.path.exists(path + filename):
            df_stock = pd.read_csv(path + filename)    
        else:
            quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
            ret, df_stock, page_req_key = quote_ctx.request_history_kline(stock, start=start_date, end=end_date)  
            quote_ctx.close() 
            df_stock['date']=pd.to_datetime(df_stock['time_key'])        
            df_stock.to_csv(path + filename) 
        df_stock.index = pd.to_datetime(df_stock.date)
        df_stock['openinterest']=0
        df_stock = df_stock[['open','high','low','close','volume','openinterest']]
        df.update({stock:df_stock})

    return df

if __name__ == "__main__":
    

    start = '2017-05-19'
    end = '2020-09-01'
    code = ['HK.00700']
    name = code
    
    df_stock = get_stockdata(code,start,end) 
    df_HSI = pd.read_csv('HSI.csv')
    df_HSI['date']=pd.to_datetime(df_HSI['time_key']) 
    df_HSI=df_HSI.set_index('date',drop=True)

    df_stock = get_stockdata(code,start,end)
    backtest = backtest.BackTest(Tech_roll, start, end, code, name,500000,bDraw = True)
    
    
    result = backtest.run()
    #result = backtest.run()
    #result = backtest.optRun(myperiod = range(50,150,20))
    print(result)
  
    #maxindex = ret[ret == ret.max()].index
    #bestResult = result.loc[maxindex,:]
    #print(bestResult.loc[:, ["夏普比率", "参数名", "参数值",  "年化收益率"]])