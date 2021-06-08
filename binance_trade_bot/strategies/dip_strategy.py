import random
import sys
import requests
import threading
import json
from datetime import datetime, timedelta
import talib.abstract as ta
import binance_trade_bot.qtpylib.indicators as qtpylib
from binance_trade_bot.models import Coin, CoinValue, CurrentCoin, Pair, ScoutHistory, Trade
import pandas as pd
from time import mktime
import time
from sqlalchemy.orm import Session

from binance_trade_bot.auto_trader import AutoTrader

class Strategy(AutoTrader):
	def initialize(self):
		super().initialize()
		self.isBacktest=type(self.manager).__name__=='MockBinanceManager'
		self.initialize_current_coin()
		self.adddfforall()

	def scout(self):
		"""
		Scout for potential jumps from the current coin to another coin
		"""
		current_coin = self.db.get_current_coin()
		ccsym=str(current_coin.symbol)
		# Display on the console, the current coin+Bridge, so users can see *some* activity and not think the bot has
		# stopped. Not logging though to reduce log size.
		if True:    print(
			f"{datetime.now()} - CONSOLE - INFO - I am scouting the best trades. "
			f"Current coin: {current_coin + self.config.BRIDGE} ",
			end="\r",
		)
		
		current_coin_price = self.manager.get_ticker_price(current_coin + self.config.BRIDGE)

		if current_coin_price is None:
			self.logger.info("Skipping scouting... current coin {} not found".format(current_coin + self.config.BRIDGE))
			return

		if ccsym self.df:
			if self.isBacktest:
				idx=self.manager.datetime - (self.manager.datetime - datetime.min) % timedelta(minutes=15)
				df = self.df[ccsym]['df'].loc[(self.df[ccsym]['df']['date'] == idx)]
				if df.empty:
					self.getdf()
					df = self.df[ccsym]['df'].loc[(self.df[ccsym]['df']['date'] == idx)]
			else:
				df = self.df[ccsym]['df'].iloc[-1:]
			if not df.empty:
				#if (ccsym in self.lastprice and current_coin_price>self.lastprice[ccsym]) and self.cansell and type(df.values[0][-1])==bool and df.values[0][-1]:# and self.isonbridge(False):
				if self.cansell and type(df.values[0][-1])==bool and df.values[0][-1]:# and self.isonbridge(False):
					#if (ccsym in self.lastprice and current_coin_price>self.lastprice[ccsym] and type(df.values[0][-1])==bool and df.values[0][-1]) and self.isonbridge(False):
					self.logger.info(f"[{datetime.now() if not self.isBacktest else self.manager.datetime}] detected dip, selling coin waiting for rise")
					if self.manager.sell_alt(current_coin, self.config.BRIDGE, current_coin_price):
						self.cansell=False
						self.lastprice[ccsym]=current_coin_price#*(self.manager.get_fee(current_coin,self.config.BRIDGE,False)*2+1)
						self.logger.info("sold coin waiting")
				elif not self.cansell and type(df.values[0][-2])==bool and df.values[0][-2] and ccsym in self.lastprice and current_coin_price<self.lastprice[ccsym]:# and self.isonbridge(True):
					self.logger.info(f"[{datetime.now() if not self.isBacktest else self.manager.datetime}] detected dip, buying coin waiting for drop")
					if self.manager.buy_alt(current_coin, self.config.BRIDGE, current_coin_price):
						self.lastprice[ccsym]=current_coin_price*(self.manager.get_fee(current_coin,self.config.BRIDGE,False)*2+1)
						self.cansell=True
						self.logger.info("bought coin back")
		self._jump_to_best_coin(current_coin, current_coin_price)

	def bridge_scout(self):
		current_coin = self.db.get_current_coin()
		if self.manager.get_currency_balance(current_coin.symbol) > self.manager.get_min_notional(
			current_coin.symbol, self.config.BRIDGE.symbol
		):
			# Only scout if we don't have enough of the current coin
			return
		new_coin = super().bridge_scout()
		if new_coin is not None:
			self.db.set_current_coin(new_coin)

	def initialize_current_coin(self):
		"""
		Decide what is the current coin, and set it up in the DB.
		"""
		if self.db.get_current_coin() is None:
			current_coin_symbol = self.config.CURRENT_COIN_SYMBOL
			if not current_coin_symbol:
				current_coin_symbol = random.choice(self.config.SUPPORTED_COIN_LIST)

			self.logger.info(f"Setting initial coin to {current_coin_symbol}")

			if current_coin_symbol not in self.config.SUPPORTED_COIN_LIST:
				sys.exit("***\nERROR!\nSince there is no backup file, a proper coin name must be provided at init\n***")
			self.db.set_current_coin(current_coin_symbol)

			# if we don't have a configuration, we selected a coin at random... Buy it so we can start trading.
			if self.config.CURRENT_COIN_SYMBOL == "":
				current_coin = self.db.get_current_coin()
				self.logger.info(f"Purchasing {current_coin} to begin trading")
				self.manager.buy_alt(
					current_coin, self.config.BRIDGE, self.manager.get_ticker_price(current_coin + self.config.BRIDGE)
				)
				self.logger.info("Ready to start trading")

	def isonbridge(self,selling=False):
		if self.isBacktest:
			return selling != True
		session: Session
		with self.db.db_session() as session:
			query = session.query(Trade).order_by(Trade.datetime.desc())
			trades: List[Trade] = query.all()
			res=trades[0].info()
			return res['state']=='COMPLETE' and res['selling']==selling and res['alt_coin']['symbol']==str(self.db.get_current_coin().symbol)

	def setlastprice(self):
		c=self.db.get_current_coin().symbol
		if self.isBacktest:
			self.lastprice[c]=0
			self.cansell=True
			return
		orders = self.manager.binance_client.get_all_orders(symbol=c+self.config.BRIDGE.symbol, limit=1)
		if len(orders)>=1:
			self.cansell=orders[0]['side']!='SELL'
			if self.cansell:
				self.lastprice[c]=float(orders[0]['price'])*(self.manager.get_fee(c,self.config.BRIDGE,False)*2+1)
			else:
				self.lastprice[c]=float(orders[0]['price'])
			self.logger.info('found old price:%s setting to:%s, ready for trading cansell:%s'%(orders[0]['price'],self.lastprice[c],self.cansell))

	def adddfforall(self):
		if not hasattr(self,'df'):
			self.df={}
		if not hasattr(self,'lastprice'):
			self.lastprice={}
		c=self.db.get_current_coin().symbol
		self.setlastprice()
		self.df[c]={}
		self.df[c]['df']=self.getdf()
		self.df[c]['ts']=datetime.now()
		if not self.isBacktest:
			t = threading.Thread(target=self.keepdfupdated, args=())
			t.daemon=True
			t.start()
		else:
			for c in self.config.SUPPORTED_COIN_LIST:
				if c not in self.df:
					self.df[c]={}
				self.df[c]['df']=self.getdf()

	def keepdfupdated(self):
		while(1):
			for c in self.config.SUPPORTED_COIN_LIST:
				c=str(c)
				if (c not in self.df) or ((datetime.now() - self.df[c]['ts']).total_seconds() / 60.0)>=15 if c!=self.db.get_current_coin().symbol else 1:
					if c not in self.df:
						self.df[c]={}
					self.df[c]['df']=self.getdf(str(c)+self.config.BRIDGE.symbol)
					self.df[c]['ts']=datetime.now()

	def getdf(self,ticker_symbol=None,interval='15m'):
		if ticker_symbol is None:
			ticker_symbol=str(self.db.get_current_coin().symbol)+self.config.BRIDGE.symbol
		#print('getting data for %s'%(ticker_symbol))
		if not self.isBacktest:
			datas=json.loads(requests.get(f'https://www.binance.com/api/v1/klines?interval={interval}&limit=1000&symbol={ticker_symbol}',headers={'user-agent':'Binance/2.30.0 (com.czzhao.binance; build:8; iOS 14.5.1) Alamofire/2.30.0'}).content)
		else:
			try:
				with open(r'C:\Users\Administrator\Desktop\bots\mlcoins\USDT15min\%s.json'%(ticker_symbol)) as fi:
					datas=[x.rstrip().split(',') for x in fi.readlines()]
			except:
				datas=json.loads(requests.get(f'https://www.binance.com/api/v1/klines?interval={interval}&limit=1000&symbol={ticker_symbol}&startTime={int(mktime(self.manager.datetime.timetuple())*1000)}',headers={'user-agent':'Binance/2.30.0 (com.czzhao.binance; build:8; iOS 14.5.1) Alamofire/2.30.0'}).content)
		data=[]
		for result in datas:
			data.append([float(result[0]),float(result[1]),float(result[2]),float(result[3]),float(result[4]),float(result[5])])
		dataframe= pd.DataFrame(data, columns = ['date','open', 'high', 'low','close', 'volume'])
		dataframe['date']=pd.to_datetime(dataframe['date']/1000,unit='s')
		dataframe['esa'] = ta.EMA(dataframe, timeperiod=14)
		dataframe['d'] = ta.EMA(abs(dataframe['close']-dataframe['esa']), timeperiod=21)
		dataframe['ci'] = (dataframe['close'] - dataframe['esa']) / (0.015 * dataframe['d'])
		dataframe['wt1'] = ta.EMA(dataframe['ci'], 21)
		dataframe['wt2'] = ta.EMA(dataframe['wt1'], 4)
		dataframe.loc[(qtpylib.crossed_above(dataframe['wt1'],dataframe['wt2'])),'buySignal']=True
		dataframe.loc[(qtpylib.crossed_below(dataframe['wt1'],dataframe['wt2'])),'sellSignal']=True
		return dataframe