import pandas as pd
import akshare as ak
import yfinance as yf
import psycopg2
from sqlalchemy import create_engine
from numpy import (float64, nan)
from abc import (abstractmethod)
import pathlib
import os
import time
from EmQuantAPI import *
import traceback
import datetime
import enum
import logging as log
import sys
import threading
from queue import Queue
from email.message import EmailMessage
import smtplib
from MyTT import *
import efinance as ef
from tqdm import tqdm


# 列名与数据对其显示
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


log.basicConfig(level=log.DEBUG,
                format='[%(asctime)s %(filename)s [line:%(lineno)d]] %(levelname)s %(message)s',
                datefmt='%d %b %Y %H:%M:%S',
                filename='myquant.log',
                filemode='a+')
logger = log.getLogger("MyQuant")
# logger.addHandler(log.StreamHandler(sys.stdout))


def append_value(dict_obj, key, value_inner):
    # Check if key exist in dict or not
    if key in dict_obj:
        # Key exist in dict.
        # Check if type of value of key is list or not
        if not isinstance(dict_obj[key], list):
            # If type is not list then make it list
            dict_obj[key] = [dict_obj[key]]
        # Append the value in list
        dict_obj[key].append(value_inner)
    else:
        # As key is not in dict,
        # so, add key-value pair
        dict_obj[key] = value_inner


def time_measure(func):
    def inner(*args, **kwargs):
        starttime = time.perf_counter()
        ret = func(*args, **kwargs)
        endtime = time.perf_counter()
        logger.debug("{} took {}s".format(str(func), endtime - starttime))
        return ret

    return inner


class CountryCode(enum.Enum):
    CHINA = 'cn'
    US = 'us'
    NONE = 'none'


exchanges = {"中小企业板": 'sz',
             "创业板": 'sz',
             "主板": 'sz',
             "科创板": 'sh',
             "主板A股": 'sh'}


class DataContext:
    country = CountryCode.NONE
    limitfordatafetched = 0
    limitfordatafetched_30 = 0
    limitfordatafetched_60 = 0
    limitfordatafetched_240 = 0
    markets = []
    marketopentime: datetime.time = None
    marketclosetime: datetime.time = None
    marketbreakstarttime: datetime.time = None
    marketbreakstoptime: datetime.time = None
    dir_name = ''
    invalid_stock_codes = set()
    sendemial_interval = 6
    strategy1 = 'strategy1'
    strategy2 = 'strategy2'
    strategy3 = 'strategy3'
    strategy4 = 'strategy4'
    strategy5 = 'strategy5'
    strategy6 = 'strategy6'
    strategy1_2 = 'strategy1and2'
    strategy1_4 = 'strategy1and4'
    strategy100 = 'strategy100'
    strategy101 = 'strategy101'
    strategy102 = 'strategy102'
    strategy103 = 'strategy103'
    strategy104 = 'strategy104'
    strategy7 = 'strategy7'
    strategy8 = 'strategy8'
    strategy9 = 'strategy9'
    strategy10 = 'strategy10'
    strategy11 = 'strategy11'
    strategy12 = 'strategy12'
    strategy13 = 'strategy13'
    strategy14 = 'strategy14'
    strategy20 = 'strategy20'
    strategy21 = 'strategy21'
    
    email_sender = 'wsx_dna@163.com'
    email_pwd = "IUCDQHTSWBWKQOPT"
    email_recipient = 'wsx_dna@sina.com'
    email_other1_recipient = 'stocash2021@163.com'
    email_other2_recipient = 'Li_hewei@126.com'
    smtp_address = "smtp.163.com"
    smtp_port = 465

    code_spotlighted = [7171, 2901, 300571, 2634, 300771, 603871, 603165, 603755, 2950, 688178,
                        603506, 603757, 537, 600167, 300765, 603327, 603360, 300738, 688026, 300800,
                        600452, 603277, 300497, 603380, 603848, 600477, 603697, 2768, 300701, 2973,
                        603639, 603357, 300640, 603053, 300246, 603203, 603040, 603657, 603530, 603458,
                        300602, 603466, 2653, 2923, 300559, 603867, 603326, 2892, 2853, 2287,
                        688289, 955, 2030, 688298, 688317, 603301, 2131, 688399, 576, 600685,
                        300030, 2382, 600683, 603985, 300246, 600026, 2838, 300206, 2567, 2310,
                        600836, 600975, 603079, 2026, 2585, 2432, 2726, 2181, 2980, 300658,
                        2950, 2157, 585, 600133, 603238, 2605, 868, 600011, 600527, 603758,
                        2487, 601991, 300443, 2223, 300210, 27, 628, 600739, 532, 601377,
                        300690, 421, 690, 987, 600961, 600198, 605358, 600460, 2151, 688126,
                        300236, 688258, 603690, 300077, 300139, 688981, 300671, 688233, 600206, 688595,
                        300706, 300333, 603005, 2371, 300493, 600667, 300661, 688123, 300548, 600360,
                        603806, 600517, 875, 601908, 601222, 601012, 601615, 603218, 27, 600008,
                        688599, 300185, 300850, 400, 300815, 625, 2266, 601877, 881]

    is_first_time_to_run = True

    @classmethod
    def initklz(cls, country_param: CountryCode):
        DataContext.country = country_param
        if DataContext.iscountryChina():
            DataContext.limitfordatafetched = 160
            DataContext.limitfordatafetched_30 = 160
            DataContext.limitfordatafetched_60 = 320
            DataContext.limitfordatafetched_240 = 128
            DataContext.markets = ["创业板", "中小企业板", "主板A股", "主板", "科创板"]
            DataContext.marketopentime = datetime.time(hour=9, minute=30)
            DataContext.marketclosetime = datetime.time(hour=15)
            DataContext.marketbreakstarttime = datetime.time(hour=11, minute=30)
            DataContext.marketbreakstoptime = datetime.time(hour=13)
            DataContext.dir_name = os.path.join(r'./result_strategy/cn', datetime.datetime.today().strftime('%Y%m%d'))
        elif DataContext.iscountryUS():
            DataContext.limitfordatafetched = 260
            DataContext.limitfordatafetched_30 = 195
            DataContext.markets = ["NASDAQ", "NYSE", "AMEX"]
            DataContext.marketopentime = datetime.time(hour=9, minute=30)
            DataContext.marketclosetime = datetime.time(hour=16)
            DataContext.dir_name = os.path.join(r'./result_strategy/us', datetime.datetime.today().strftime('%Y%m%d'))
            DataContext.invalid_stock_codes = {"AMCI.O", "BDGE.O", "BEAT.O", "FSDC.O", "GTLS.O", "HFEN.O", "INAQ.O",
                                               "NOVS.O", "PEIX.O", "YRCW.O", "CCC.N", "CTRA.N", "PCPL.N", "SALT.N",
                                               "CRMD.A", "CTO.A", "MCAC.O", "PANA.N", "OBLG.A", "LGVW.N", "XAN_C.N",
                                               "XAN.N", "WYND.N", "CVLB.O"}

    @classmethod
    def iscountryChina(cls):
        return DataContext.country == CountryCode.CHINA

    @classmethod
    def iscountryUS(cls):
        return DataContext.country == CountryCode.US

    def __init__(self):
        self.start_i = -1
        self.end_i = -1
        self.cross_sma_period = 70
        self.rsv_period = 9
        self.k_period = 3
        self.d_period = 3
        self.obv_period = 70
        self.obv_a_period = 30
        self.queue = Queue()

        if not os.path.isdir(DataContext.dir_name):
            os.mkdir(DataContext.dir_name)

        if DataContext.iscountryChina():
            self.greater_than_sma_period = 80

            # 15mins
            self.China_small_15 = StockData('small')
            self.China_startup_15 = StockData('startup')
            self.China_tech_startup_15 = StockData('tech_startup')
            self.China_sh_a_15 = StockData('sh_a')
            self.China_sz_a_15 = StockData('sz_a')

            # 30mins
            self.China_small_30 = StockData('small')
            self.China_startup_30 = StockData('startup')
            self.China_tech_startup_30 = StockData('tech_startup')
            self.China_sh_a_30 = StockData('sh_a')
            self.China_sz_a_30 = StockData('sz_a')

            # 60mins
            self.China_small_60 = StockData('small')
            self.China_startup_60 = StockData('startup')
            self.China_tech_startup_60 = StockData('tech_startup')
            self.China_sh_a_60 = StockData('sh_a')
            self.China_sz_a_60 = StockData('sz_a')

            # 240mins
            self.China_small_240 = StockData('small')
            self.China_startup_240 = StockData('startup')
            self.China_tech_startup_240 = StockData('tech_startup')
            self.China_sh_a_240 = StockData('sh_a')
            self.China_sz_a_240 = StockData('sz_a')

            # symbol lists
            self.symbols_l_tech_startup = []
            self.symbols_l_startup = []
            self.symbols_l_small = []
            self.symbols_l_sh_a = []
            self.symbols_l_sz_a = []

            # symbol.exchange lists
            self.symbols_exchange_l_tech_startup = []
            self.symbols_exchange_l_startup = []
            self.symbols_exchange_l_small = []
            self.symbols_exchange_l_sh_a = []
            self.symbols_exchange_l_sz_a = []

            self.preparedata("科创板")
            self.preparedata("中小企业板")
            self.preparedata("创业板")
            self.preparedata("主板A股")
            self.preparedata("主板")

            self.data15mins = {stock_group["科创板"]: self.China_tech_startup_15,
                               stock_group["中小企业板"]: self.China_small_15,
                               stock_group["创业板"]: self.China_startup_15,
                               stock_group["主板A股"]: self.China_sh_a_15,
                               stock_group["主板"]: self.China_sz_a_15}

            self.data30mins = {stock_group["科创板"]: self.China_tech_startup_30,
                               stock_group["中小企业板"]: self.China_small_30,
                               stock_group["创业板"]: self.China_startup_30,
                               stock_group["主板A股"]: self.China_sh_a_30,
                               stock_group["主板"]: self.China_sz_a_30}

            self.data60mins = {stock_group["科创板"]: self.China_tech_startup_60,
                               stock_group["中小企业板"]: self.China_small_60,
                               stock_group["创业板"]: self.China_startup_60,
                               stock_group["主板A股"]: self.China_sh_a_60,
                               stock_group["主板"]: self.China_sz_a_60}

            self.data240mins = {stock_group["科创板"]: self.China_tech_startup_240,
                               stock_group["中小企业板"]: self.China_small_240,
                               stock_group["创业板"]: self.China_startup_240,
                               stock_group["主板A股"]: self.China_sh_a_240,
                               stock_group["主板"]: self.China_sz_a_240}

            self.symbols = {stock_group["科创板"]: self.symbols_l_tech_startup,
                            stock_group["中小企业板"]: self.symbols_l_small,
                            stock_group["创业板"]: self.symbols_l_startup,
                            stock_group["主板A股"]: self.symbols_l_sh_a,
                            stock_group["主板"]: self.symbols_l_sz_a}

            self.symbols_exchange = {stock_group["科创板"]: self.symbols_exchange_l_tech_startup,
                                     stock_group["中小企业板"]: self.symbols_exchange_l_small,
                                     stock_group["创业板"]: self.symbols_exchange_l_startup,
                                     stock_group["主板A股"]: self.symbols_exchange_l_sh_a,
                                     stock_group["主板"]: self.symbols_exchange_l_sz_a}
        elif DataContext.iscountryUS():
            self.greater_than_sma_period = 130

            # 15mins
            self.US_nasdaq_15 = StockData(stock_group["NASDAQ"])
            self.US_nyse_15 = StockData(stock_group['NYSE'])
            self.US_amex_15 = StockData(stock_group['AMEX'])

            # 30mins
            self.US_nasdaq_30 = StockData(stock_group["NASDAQ"])
            self.US_nyse_30 = StockData(stock_group['NYSE'])
            self.US_amex_30 = StockData(stock_group['AMEX'])

            # symbol lists
            self.symbols_l_nasdaq = []
            self.symbols_l_nyse = []
            self.symbols_l_amex = []

            # symbol.exchange lists
            self.symbols_exchange_l_nasdaq = []
            self.symbols_exchange_l_nyse = []
            self.symbols_exchange_l_amex = []

            self.preparedata("NASDAQ")
            self.preparedata("NYSE")
            self.preparedata("AMEX")

            self.data15mins = {stock_group["NASDAQ"]: self.US_nasdaq_15,
                               stock_group["NYSE"]: self.US_nyse_15,
                               stock_group["AMEX"]: self.US_amex_15}

            self.data30mins = {stock_group["NASDAQ"]: self.US_nasdaq_30,
                               stock_group["NYSE"]: self.US_nyse_30,
                               stock_group["AMEX"]: self.US_amex_30}

            self.symbols = {stock_group["NASDAQ"]: self.symbols_l_nasdaq,
                            stock_group["NYSE"]: self.symbols_l_nyse,
                            stock_group["AMEX"]: self.symbols_l_amex}

            self.symbols_exchange = {stock_group["NASDAQ"]: self.symbols_exchange_l_nasdaq,
                                     stock_group["NYSE"]: self.symbols_exchange_l_nyse,
                                     stock_group["AMEX"]: self.symbols_exchange_l_amex}

        self.sendemailtime: datetime.datetime = None
        self.totalresult = {DataContext.strategy1_4: {}, DataContext.strategy1_2: {},
                            DataContext.strategy5: {}, DataContext.strategy100: {},
                            DataContext.strategy4: {}, DataContext.strategy1: {},
                            DataContext.strategy2: {}, DataContext.strategy3: {},
                            DataContext.strategy6: {}, DataContext.strategy7: {},
                            DataContext.strategy8: {}, DataContext.strategy9: {},
                            DataContext.strategy10: {}, DataContext.strategy11: {},
                            DataContext.strategy12: {}, DataContext.strategy13: {},
                            DataContext.strategy14: {}, DataContext.strategy20: {},
                            DataContext.strategy21: {}}
        self.sectors = {}

        logger.debug("Initialization of context is done.")

    def preparedata(self, indicator: str):
        logger.debug("--- Start to prepare data --- " + indicator)
        tablename_prefixes = ["", "", "", ""]

        def gentablenameprefix(prefixes: list):
            if DataContext.iscountryChina():
                countrycode = 'china_'
            else:
                countrycode = 'us_'
            if indicator == "科创板":
                prefixes[0] = 'china_tbl_'
            else:
                prefixes[0] = countrycode + stock_group[indicator] + '_tbl_'
            prefixes[1] = countrycode + stock_group[indicator] + '_tbl_30_'
            prefixes[2] = countrycode + stock_group[indicator] + '_tbl_60_'
            prefixes[3] = countrycode + stock_group[indicator] + '_tbl_240_'

        if indicator in {"中小企业板", "创业板", "主板"}:
            header = "公司代码"
        elif indicator in {"科创板", "主板A股"}:
            header = 'SECURITY_CODE_A'
        elif indicator in {"NASDAQ", "NYSE", "AMEX"}:
            header = 'SECURITY_CODE_A'
        exchange = exchanges[indicator]

        gentablenameprefix(tablename_prefixes)

        tmp_symbol_l = []
        tmp_symbol_exchange_l = []
        tmp_data = StockData()
        tmp_data.sector = stock_group[indicator]
        tmp_data_30 = StockData()
        tmp_data_30.sector = stock_group[indicator]
        tmp_data_60 = StockData()
        tmp_data_60.sector = stock_group[indicator]
        tmp_data_240 = StockData()
        tmp_data_240.sector = stock_group[indicator]

        def getdatafromdatabase(tablename: str, limits: int):
            sql_statement = "select * from \"{}\" order by crt_time desc limit {};".format(tablename, limits)
            datafromdatabase = pd.read_sql_query(sql_statement, engine, index_col='crt_time')
            datafromdatabase.sort_index(inplace=True)
            return datafromdatabase

        symbol_path = symbol_paths[stock_group[indicator]]
        if pathlib.Path(symbol_path).is_file():
            symbolsfromcsv = pd.read_csv(symbol_path)
            if DataContext.iscountryChina():
                tmp_symbol_l = symbolsfromcsv[header].astype(str).str.zfill(6).tolist()
            elif DataContext.iscountryUS():
                tmp_symbol_l = symbolsfromcsv[header].astype(str).tolist()
            for symbol in tmp_symbol_l:
                if DataContext.iscountryChina():
                    internal_symbol = ".".join([symbol, exchange])
                    tmp_symbol_exchange_l.append(internal_symbol)
                elif DataContext.iscountryUS():
                    symbol_exchange_l = symbol.split(".")
                    if len(symbol_exchange_l) > 1:
                        symbol = symbol_exchange_l[0]
                        tmp_symbol_exchange_l.append(symbol)
                    else:
                        logger.error('%s is invalid', symbol)
                        continue
                table_name = tablename_prefixes[0] + symbol
                tmp_data.update(symbol, getdatafromdatabase(table_name, DataContext.limitfordatafetched))
                table_name_30 = tablename_prefixes[1] + symbol
                tmp_data_30.update(symbol, getdatafromdatabase(table_name_30, DataContext.limitfordatafetched_30))
                if DataContext.iscountryChina():
                    table_name_60 = tablename_prefixes[2] + symbol
                    tmp_data_60.update(symbol, getdatafromdatabase(table_name_60, DataContext.limitfordatafetched_60))
                    table_name_240 = tablename_prefixes[3] + symbol
                    tmp_data_240.update(symbol, getdatafromdatabase(table_name_240, DataContext.limitfordatafetched_240))
        else:
            logger.error('%s does not exist', (symbol_paths[stock_group[indicator]]))
            exit()
        if DataContext.iscountryChina():
            if indicator == "中小企业板":
                self.symbols_l_small = tmp_symbol_l
                self.symbols_exchange_l_small = tmp_symbol_exchange_l
                self.China_small_15 = tmp_data
                self.China_small_30 = tmp_data_30
                self.China_small_60 = tmp_data_60
                self.China_small_240 = tmp_data_240
            elif indicator == "创业板":
                self.symbols_l_startup = tmp_symbol_l
                self.symbols_exchange_l_startup = tmp_symbol_exchange_l
                self.China_startup_15 = tmp_data
                self.China_startup_30 = tmp_data_30
                self.China_startup_60 = tmp_data_60
                self.China_startup_240 = tmp_data_240
            elif indicator == "科创板":
                self.symbols_l_tech_startup = tmp_symbol_l
                self.symbols_exchange_l_tech_startup = tmp_symbol_exchange_l
                self.China_tech_startup_15 = tmp_data
                self.China_tech_startup_30 = tmp_data_30
                self.China_tech_startup_60 = tmp_data_60
                self.China_tech_startup_240 = tmp_data_240
            elif indicator == "主板A股":
                self.symbols_l_sh_a = tmp_symbol_l
                self.symbols_exchange_l_sh_a = tmp_symbol_exchange_l
                self.China_sh_a_15 = tmp_data
                self.China_sh_a_30 = tmp_data_30
                self.China_sh_a_60 = tmp_data_60
                self.China_sh_a_240 = tmp_data_240
            elif indicator == "主板":
                self.symbols_l_sz_a = tmp_symbol_l
                self.symbols_exchange_l_sz_a = tmp_symbol_exchange_l
                self.China_sz_a_15 = tmp_data
                self.China_sz_a_30 = tmp_data_30
                self.China_sz_a_60 = tmp_data_60
                self.China_sz_a_240 = tmp_data_240
        elif DataContext.iscountryUS():
            if indicator == "NASDAQ":
                self.symbols_l_nasdaq = tmp_symbol_exchange_l
                self.symbols_exchange_l_nasdaq = tmp_symbol_l
                self.US_nasdaq_15 = tmp_data
                self.US_nasdaq_30 = tmp_data_30
            elif indicator == "NYSE":
                self.symbols_l_nyse = tmp_symbol_exchange_l
                self.symbols_exchange_l_nyse = tmp_symbol_l
                self.US_nyse_15 = tmp_data
                self.US_nyse_30 = tmp_data_30
            elif indicator == "AMEX":
                self.symbols_l_amex = tmp_symbol_exchange_l
                self.symbols_exchange_l_amex = tmp_symbol_l
                self.US_amex_15 = tmp_data
                self.US_amex_30 = tmp_data_30
        logger.debug("--- It is done with preparation of data --- " + indicator)

@time_measure
def csqsnapshot_t(codes, indicators, options=""):
    return c.csqsnapshot(codes, indicators, options)


connections = threading.local()

stock_group = {"科创板": 'tech_startup',
               "中小企业板": 'small',
               "创业板": 'startup',
               "主板A股": 'sh_a',
               "主板": 'sz_a',
               "NASDAQ": 'nasdaq',
               "NYSE": 'nyse',
               "AMEX": 'amex'}
columns = ['gid', 'open', 'close', 'high', 'low', 'volume', 'time', 'isGreater']
root_path = r'/Users/shicaidonghua/Documents/stocks/quant_akshare/'
symbol_paths = {'small': root_path + 'small_symbols.csv',
                'startup': root_path + 'startup_symbols.csv',
                'tech_startup': root_path + 'tech_startup_symbols.csv',
                'sh_a': root_path + 'sh_a_symbols.csv',
                'sz_a': root_path + 'sz_a_symbols.csv',
                'nasdaq': root_path + 'nasdaq_symbols.csv',
                'nyse': root_path + 'nyse_symbols.csv',
                'amex': root_path + 'amex_symbols.csv'}

time_windows_15 = [0 for i in range(100)]  # set 100 so as to test after market
time_windows_30 = [0 for i in range(100)]  # set 100 so as to test after market
time_windows_60 = [0 for i in range(100)]  # set 100 so as to test after market


sectors_CN = {'000001': "优选股关注",
              '007180': "券商概念",
              '007224': "大飞机",
              '007315': "半导体",
              '007205': "国产芯片",
              '007039': "生物疫苗",
              '007001': "军工",
              '007139': "医疗器械",
              '007146': "病毒防治",
              '007147': "独家药品",
              '007162': "基因测序",
              '007167': "免疫治疗",
              '007188': "健康中国",
              '007195': "人工智能",
              '007200': "区块链",
              '007206': "新能源车",
              '007212': "生物识别",
              '007218': "精准医疗",
              '007220': "军民融合",
              '007243': "互联医疗",
              '007246': "体外诊断",
              '007284': "数字货币",
              '007332': "长寿药",
              '007336': "疫苗冷链",
              '007339': "肝素概念",
              '014010018003': "生物医药",
              '004012003001': "太阳能",
              '015011003003': "光伏",
              '007371': "低碳冶金",
              '018001001002001': "新能源设备与服务",
              '007068': "太阳能",
              '007005': "节能环保",
              '007152': "燃料电池",
              '007307': "HIT电池",
              '007370': "光伏建筑一体化",
              '007369': "碳化硅",
              '007003': "煤化工",
              '007004': "新能源",
              '007007': "AB股",
              '007008': "AH股",
              '007009': "HS300_",
              '007010': "次新股",
              '007013': "中字头",
              '007014': "创投",
              '007017': "网络游戏",
              '007019': "ST股",
              '007020': "化工原料",
              '007022': "参股券商",
              '007024': "稀缺资源",
              '007025': "社保重仓",
              '007028': "新材料",
              '007029': "参股期货",
              '007030': "参股银行",
              '007032': "转债标的",
              '007033': "成渝特区",
              '007034': "QFII重仓",
              '007035': "基金重仓",
              '007038': "黄金概念",
              '007040': "深圳特区",
              '007043': "机构重仓",
              '007045': "物联网",
              '007046': "移动支付",
              '007048': "油价相关",
              '007049': "滨海新区",
              '007050':"股权激励",
              '007051': "深成500",
              '007053': "预亏预减",
              '007054': "预盈预增",
              '007057': "锂电池",
              '007058': "核能核电",
              '007059': "稀土永磁",
              '007060': "云计算",
              '007061': "LED",
              '007062': "智能电网",
              '007072': "铁路基建",
              '007074': "长江三角",
              '007075': "风能",
              '007076': "融资融券",
              '007077': "水利建设",
              '007079': "新三板",
              '007080': "海工装备",
              '007082': "页岩气",
              '007083': "参股保险",
              '007085': "油气设服",
              '007089': "央视50_",
              '007090': "上证50_",
              '007091': "上证180_",
              '007093': "食品安全",
              '007094': "中药",
              '007096': "石墨烯",
              '007098': "3D打印",
              '007099': "地热能",
              '007100': "海洋经济",
              '007102': "通用航空",
              '007104': "智慧城市",
              '007105': "北斗导航",
              '007108': "土地流转",
              '007109': "送转预期",
              '007110': "大数据",
              '007111': "中超概念",
              '007112': "B股",
              '007113': "互联金融",
              '007114': "创业成份",
              '007116': "智能机器",
              '007117': "智能穿戴",
              '007118': "手游概念",
              '007119': "上海自贸",
              '007120': "特斯拉",
              '007122': "养老概念",
              '007124': "网络安全",
              '007125': "智能电视",
              '007131': "在线教育",
              '007133': "二胎概念",
              '007137': "电商概念",
              '007136': "苹果概念",
              '007138': "国家安防",
              '007140': "生态农业",
              '007142': "彩票概念",
              '007143': "沪企改革",
              '007145': "蓝宝石",
              '007148': "粤港自贸",
              '007149': "超导概念",
              '007150': "智能家居",
              '007153': "国企改革",
              '007154': "京津冀",
              '007155': "举牌",
              '007159': "阿里概念",
              '007160': "氟化工",
              '007161': "在线旅游",
              '007164': "小金属",
              '007165': "国产软件",
              '007166': "IPO受益",
              '007168': "全息技术",
              '007169': "充电桩",
              '007170': "中证500",
              '007172': "超级电容",
              '007173': "无人机",
              '007174': "上证380",
              '007175': "人脑工程",
              '007176': "沪股通",
              '007177': "体育产业",
              '007178': "赛马概念",
              '007179': "量子通信",
              '007181': "一带一路",
              '007182': "2025规划",
              '007183': "5G概念",
              '007184': "航母概念",
              '007186': "北京冬奥",
              '007187': "证金持股",
              '007190': "PPP模式",
              '007191': "虚拟现实",
              '007192': "高送转",
              '007193': "海绵城市",
              '007196': "增强现实",
              '007197': "无人驾驶",
              '007198': "工业4.0",
              '007199': "壳资源",
              '007201': "OLED",
              '007202': "单抗概念",
              '007203': "3D玻璃",
              '007204': "猪肉概念",
              '007207': "车联网",
              '007209': "网红直播",
              '007210': "草甘膦",
              '007211': "无线充电",
              '007213': "债转股",
              '007214': "快递概念",
              '007215': "股权转让",
              '007216': "深股通",
              '007217': "钛白粉",
              '007219': "共享经济",
              '007221': "超级品牌",
              '007222': "贬值受益",
              '007223': "雄安新区",
              '007225': "昨日涨停",
              '007226': "昨日连板",
              '007227': "昨日触板",
              '007228': "可燃冰",
              '007230': "MSCI中国",
              '007231': "创业板综",
              '007232': "深证100R",
              '007233': "租售同权",
              '007234': "养老金",
              '007236': "新零售",
              '007237': "万达概念",
              '007238': "工业互联",
              '007239': "小米概念",
              '007240': "乡村振兴",
              '007241': "独角兽",
              '007244': "东北振兴",
              '007245': "知识产权",
              '007247': "富士康",
              '007248': "天然气",
              '007249': "百度概念",
              '007251': "影视概念",
              '007253': "京东金融",
              '007254': "进口博览",
              '007255': "纾困概念",
              '007256': "冷链物流",
              '007257': "电子竞技",
              '007258': "华为概念",
              '007259': "纳米银",
              '007260': "工业大麻",
              '007263': "超清视频",
              '007264': "边缘计算",
              '007265': "数字孪生",
              '007266': "超级真菌",
              '007268': "氢能源",
              '007269': "电子烟",
              '007270': "人造肉",
              '007271': "富时罗素",
              '007272': "GDR",
              '007275': "青蒿素",
              '007276': "垃圾分类",
              '007278': "ETC",
              '007280': "PCB",
              '007281': "分拆预期",
              '007282': "标准普尔",
              '007283': "UWB概念",
              '007285': "光刻胶",
              '007286': "VPN",
              '007287': "智慧政务",
              '007288': "鸡肉概念",
              '007289': "农业种植",
              '007290': "医疗美容",
              '007291': "MLCC",
              '007292': "乳业",
              '007293': "无线耳机",
              '007294': "阿兹海默",
              '007295': "维生素",
              '007296': "白酒",
              '007297': "IPv6",
              '007298': "胎压监测",
              '007299': "CRO",
              '007300': "3D摄像头",
              '007301': "MiniLED",
              '007302': "云游戏",
              '007303': "广电",
              '007304': "传感器",
              '007305': "流感",
              '007306': "转基因",
              '007308': "降解塑料",
              '007309': "口罩",
              '007310': "远程办公",
              '007311': "消毒剂",
              '007312': "医废处理",
              '007313': "WiFi",
              '007314': "氮化镓",
              '007316': "特高压",
              '007317': "RCS概念",
              '007318': "天基互联",
              '007319': "数据中心",
              '007320': "字节概念",
              '007321': "地摊经济",
              '007322': "三板精选",
              '007323': "湖北自贸",
              '007324': "免税概念",
              '007325': "抖音小店",
              '007326': "地塞米松",
              '007328': "尾气治理",
              '007329': "退税商店",
              '007330': "蝗虫防治",
              '007331': "中芯概念",
              '007333': "蚂蚁概念",
              '007334': "代糖概念",
              '007335': "辅助生殖",
              '007337': "商汤概念",
              '007338': "汽车拆解",
              '007340': "装配建筑",
              '007341': "EDA概念",
              '007342': "屏下摄像",
              '007343': "MicroLED",
              '007344': "氦气概念",
              '007345': "刀片电池",
              '007346': "第三代半导体",
              '007347': "鸿蒙概念",
              '007348': "盲盒经济",
              '007349': "C2M概念",
              '007350': "eSIM",
              '007351': "拼多多概念",
              '007352': "虚拟电厂",
              '007353': "数字阅读",
              '007354': "有机硅",
              '007355': "RCEP概念",
              '007356': "航天概念",
              '007357': "6G概念",
              '007358': "社区团购",
              '007359': "碳交易",
              '007360': "水产养殖",
              '007361': "固态电池",
              '007362': "汽车芯片",
              '007363': "注册制次新股",
              '007364': "快手概念",
              '007365': "注射器概念",
              '007366': "化妆品概念",
              '007367': "磁悬浮概念",
              '007368': "被动元件",
              '007372': "工业气体",
              # There is unavailable value of gain/loss and money flow for the below sectors
              '007373': "电子车牌",
              '007374': "核污染防治",
              '007375': "华为汽车",
              '007376': "换电概念",
              '007377': "CAR - T细胞疗法",
              '073259': "碳交易"}


sectors_US = {'000001': "优选股关注",
              '201001': "中概股"}


# param: echo=True that is used to show each sql statement used in query
engine = create_engine("postgresql+psycopg2://Raymond:123123@localhost:5432/Raymond", encoding='utf-8')

class DataSource(enum.Enum):
    EAST_MONEY = 0
    AK_SHARE = 1
    YAHOO = 2
    SNAPSHOT = 3
    EFINANCE = 4


def getdbconn():
    if 'connection' not in connections.__dict__:
        connections.connection = psycopg2.connect(
            user="Raymond",
            password="123123",
            host="127.0.0.1",
            port="5432",
            database="Raymond")
        logger.info('Connect to Raymond\'s database - {}\n current connection is {}\n thread ident is {} and native thread id is {}\n'.
                    format(connections.connection.get_dsn_parameters(), connections.connection, threading.get_ident(), threading.get_native_id()))
    return connections.connection


def createtable(symbols: list, exchange: str, period: int):
    conn = getdbconn()
    csr = getdbconn().cursor()
    stock_name_array = map(str, symbols)
    symbols_t = ','.join(stock_name_array)
    stock_symbols = '{' + symbols_t + '}'
    logger.debug('%s - %s' % (exchange, stock_symbols))
    statement_sql = ""
    create_table = ""
    if DataContext.iscountryChina():
        if period == 15:
            create_table = "create_table_c"
        elif period == 30:
            create_table = "create_table_c_30"
        elif period == 60:
            create_table = "create_table_c_60"
        elif period == 240:
            create_table = "create_table_c_240"
    elif DataContext.iscountryUS():
        if period == 15:
            create_table = "create_table_u"
        elif period == 30:
            create_table = "create_table_u_30"
    if create_table != "":
        statement_sql = "call " + create_table + "(%s,%s);"
    csr.execute(statement_sql, (stock_symbols, exchange))
    conn.commit()


def droptable(symbols: list, exchange: str):
    conn = getdbconn()
    csr = getdbconn().cursor()
    stock_name_df_array = map(str, symbols)
    symbols_t = ','.join(stock_name_df_array)
    stock_symbols = '{' + symbols_t + '}'
    logger.debug('%s - %s' % (exchange, stock_symbols))
    csr.execute("call drop_table_c(%s,%s);", (stock_symbols, exchange))
    conn.commit()

update_stat = " do update set open=excluded.open,close=excluded.close,high=excluded.high,low=excluded.low,volume=excluded.volume;"
do_nothing = " do nothing;"

def inserttab(exchange: str, symbol: str, stock_df: pd.DataFrame, datasource: DataSource, period=15, transientdf: pd.DataFrame=None, type_func=1):
    conn = getdbconn()
    csr = getdbconn().cursor()
    if DataContext.iscountryChina():
        if datasource == DataSource.AK_SHARE:
            stock_day = stock_df['day'].tolist()
            header_o = 'open'
            header_c = 'close'
            header_h = 'high'
            header_l = 'low'
            header_v = 'volume'
        elif datasource == DataSource.SNAPSHOT:
            stock_day = stock_df.index.tolist()
            header_o = 'open'
            header_c = 'close'
            header_h = 'high'
            header_l = 'low'
            header_v = 'volume'
        elif datasource == DataSource.EAST_MONEY:
            header_o = 'OPEN'
            header_c = 'CLOSE'
            header_h = 'HIGH'
            header_l = 'LOW'
            header_v = 'VOLUME'
            if type_func == 1:
                stock_day = stock_df.index.tolist()
            elif type_func == 2:
                header_d = 'DATES'
        elif datasource == DataSource.EFINANCE:
            stock_day = stock_df['日期'].tolist()
            header_o = '开盘'
            header_c = '收盘'
            header_h = '最高'
            header_l = '最低'
            header_v = '成交量'
        statement_start = "insert into china_"
    elif DataContext.iscountryUS():
        if datasource == DataSource.YAHOO:
            stock_day = stock_df.index.tolist()
            header_o = 'Open'
            header_c = 'Close'
            header_h = 'High'
            header_l = 'Low'
            header_v = 'Volume'
        statement_start = "insert into us_"
    stock_open = stock_df[header_o]
    stock_close = stock_df[header_c]
    stock_high = stock_df[header_h]
    stock_low = stock_df[header_l]
    stock_volume = list(map(int, stock_df[header_v].tolist()))
    if period == 15:
        count: int = 0
        for each_time in stock_day:
            if DataContext.iscountryUS():
                csr.execute(statement_start + exchange + "_tbl (gid,crt_time,open,close,high,low,volume) " +
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + update_stat,
                            (str(symbol), str(each_time), "{:.4f}".format(stock_open[count]), "{:.4f}".format(stock_close[count]),
                            "{:.4f}".format(stock_high[count]), "{:.4f}".format(stock_low[count]), str(stock_volume[count])))
            elif DataContext.iscountryChina():
                csr.execute(statement_start + exchange + "_tbl (gid,crt_time,open,close,high,low,volume) " +
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + update_stat,
                            (str(symbol), str(each_time), str(stock_open[count]), str(stock_close[count]),
                             str(stock_high[count]), str(stock_low[count]), str(stock_volume[count])))
            count += 1
        conn.commit()
        logger.debug("%s - rows are %d for period 15 mins" % (symbol, count))
    elif period == 30:
        count: int = 0
        i: int = 0
        loop_len = len(stock_day) - 1
        while i < loop_len:
            timestamp: pd.Timestamp = pd.to_datetime(stock_day[i])
            time_point = datetime.datetime(year=timestamp.year, month=timestamp.month, day=timestamp.day,
                                           hour=timestamp.hour, minute=timestamp.minute, second=timestamp.second)
            open_time = datetime.datetime.combine(datetime.date(year=timestamp.year, month=timestamp.month, day=timestamp.day),
                                                  DataContext.marketopentime)
            count_period = (time_point - open_time).seconds // (15 * 60)
            if i == 0 and (count_period % 2) == 0:
                i += 1
                continue
            next_idx = i + 1
            open_value = stock_open[i]
            close_value = stock_close[next_idx]
            if stock_high[i] >= stock_high[next_idx]:
                high_value = stock_high[i]
            else:
                high_value = stock_high[next_idx]
            if stock_low[i] <= stock_low[next_idx]:
                low_value = stock_low[i]
            else:
                low_value = stock_low[next_idx]
            volume_value = stock_volume[i] + stock_volume[next_idx]
            i += 2
            if DataContext.iscountryUS():
                csr.execute(statement_start + exchange + "_tbl_30 (gid,crt_time,open,close,high,low,volume) " +
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + "_30" + update_stat,
                            (str(symbol), str(stock_day[next_idx]), "{:.4f}".format(open_value), "{:.4f}".format(close_value),
                            "{:.4f}".format(high_value), "{:.4f}".format(low_value), str(volume_value)))
            elif DataContext.iscountryChina():
                csr.execute(statement_start + exchange + "_tbl_30 (gid,crt_time,open,close,high,low,volume) " +
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + "_30" + update_stat,
                            (str(symbol), str(stock_day[next_idx]), str(open_value), str(close_value),
                             str(high_value), str(low_value), str(volume_value)))
                transientdf.loc[len(transientdf)] = [str(symbol), open_value, close_value, high_value, low_value,
                                                     str(volume_value), stock_day[next_idx], nan]
            count += 1
        if transientdf is not None:
            transientdf.set_index('time', inplace=True)
        conn.commit()
        logger.debug("%s - rows are %d for period 30 mins" % (symbol, count))
    elif period == 60 and DataContext.iscountryChina():
        count: int = 0
        transientdf.sort_index(inplace=True)
        stock_day = transientdf.index.tolist()
        stock_open = transientdf['open']
        stock_close = transientdf['close']
        stock_high = transientdf['high']
        stock_low = transientdf['low']
        stock_volume = list(map(int, transientdf['volume'].tolist()))
        i: int = 0
        loop_len = len(stock_day) - 1
        while i < loop_len:
            next_idx = i + 1
            open_value = stock_open[i]
            close_value = stock_close[next_idx]
            if stock_high[i] >= stock_high[next_idx]:
                high_value = stock_high[i]
            else:
                high_value = stock_high[next_idx]
            if stock_low[i] <= stock_low[next_idx]:
                low_value = stock_low[i]
            else:
                low_value = stock_low[next_idx]
            volume_value = stock_volume[i] + stock_volume[next_idx]
            i += 2
            csr.execute(statement_start + exchange + "_tbl_60 (gid,crt_time,open,close,high,low,volume) " +
                        "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + "_60" + update_stat,
                        (str(symbol), str(stock_day[next_idx]), str(open_value), str(close_value),
                         str(high_value), str(low_value), str(volume_value)))
            count += 1
        conn.commit()
        logger.debug("%s - rows are %d for period 60 mins" % (symbol, count))
    elif period == 240 and DataContext.iscountryChina():
        count: int = 0
        if type_func == 2:
            for code, row in stock_df.iterrows():
                if row[header_o] is not None and \
                   row[header_c] is not None and \
                   row[header_h] is not None and \
                   row[header_l] is not None and \
                   row[header_v] is not None and \
                   len(code.split('.')) > 1:
                    csr.execute(statement_start + exchange + "_tbl_240 (gid,crt_time,open,close,high,low,volume) " +
                                "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + "_240" + update_stat,
                                (str(code.split('.')[0]), str(row[header_d]), str(row[header_o]), str(row[header_c]),
                                 str(row[header_h]), str(row[header_l]), str(row[header_v])))
                    count += 1
        else:
            transientdf.sort_index(inplace=True)
            stock_day = transientdf.index.tolist()
            stock_open = transientdf['open']
            stock_close = transientdf['close']
            stock_high = transientdf['high']
            stock_low = transientdf['low']
            stock_volume = list(map(int, transientdf['volume'].tolist()))
            i: int = 0
            stock_day_len = len(stock_day)
            # the transientdf contains data of 30 mins
            abandoned_15_mins_count = stock_day_len % 8
            if abandoned_15_mins_count != 0:
                i += abandoned_15_mins_count
            while i < stock_day_len:
                last_index = i + 7
                if last_index > stock_day_len - 1:
                    break
                timestamp: pd.Timestamp = pd.to_datetime(stock_day[i])
                time_point = datetime.datetime(year=timestamp.year, month=timestamp.month, day=timestamp.day)
                open_value = stock_open[i]
                close_value = stock_close[last_index]
                high_value = float(stock_high[i])
                low_value = float(stock_low[i])
                volume_value = stock_volume[i]
                i += 1
                while i < last_index + 1:
                    if float(stock_high[i]) > high_value:
                        high_value = float(stock_high[i])
                    if float(stock_low[i]) < low_value:
                        low_value = float(stock_low[i])
                    volume_value += stock_volume[i]
                    i += 1
                csr.execute(statement_start + exchange + "_tbl_240 (gid,crt_time,open,close,high,low,volume) " +
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + "_240" + update_stat,
                            (str(symbol), str(time_point), str(open_value), str(close_value),
                             str(high_value), str(low_value), str(volume_value)))
                count += 1

        if count != 0:
            conn.commit()
            logger.debug("%s - rows are %d for period 240 mins" % (exchange, count))


pbar = tqdm()


def insertdata(exchange: str, group: str, symbols: list, retried, datasource: DataSource, period: str = '15',
               type_func=1, context: DataContext=None, adjust: str = "qfq"):
    exchange_group = ",".join([exchange, group])

    def update_database(exchange_in: str, symbol_s: str, dataset, source):
        inserttab(exchange_in, symbol_s, dataset, source)
        tmp_df = pd.DataFrame(columns=columns)
        inserttab(exchange_in, symbol_s, dataset, source, period=30, transientdf=tmp_df)
        inserttab(exchange_in, symbol_s, dataset, source, period=60, transientdf=tmp_df)
        inserttab(exchange_in, symbol_s, dataset, source, period=240, transientdf=tmp_df)

    if DataContext.iscountryChina():
        evt = threading.Event()
        if type_func == 1:
            global pbar
            pbar.total = len(symbols)
            pbar.set_description_str(f'{exchange} Processing')
            for symbol_i in symbols:
                if datasource == DataSource.AK_SHARE:
                    symbol_internal = group + str(symbol_i)
                    # stock_zh_df_tmp = ak.stock_zh_a_minute(symbol=symbol_internal, period=period, adjust=adjust)
                    # FIXME
                    try:
                        time.sleep(1)
                        stock_zh_df_tmp = ak.stock_zh_a_minute(symbol=symbol_internal, period=period, datalengh="16")
                    except:
                        stock_zh_df_tmp = pd.DataFrame(columns=columns)
                        failed_to_get_data_symbols.append((group, str(symbol_i)))
                        logger.error("it is failed to get stock data for {}".format(symbol_internal))
                elif datasource == DataSource.EAST_MONEY:
                    symbol_internal = ".".join([str(symbol_i), group])
                    stock_zh_df_tmp = c.cmc(symbol_internal, "OPEN,HIGH,LOW,CLOSE,VOLUME,TIME",
                                            (datetime.datetime.today() - datetime.timedelta(days=3)).strftime(
                                                "%Y-%m-%d"),
                                            datetime.datetime.today().strftime("%Y-%m-%d"),
                                            "AdjustFlag=1,RowIndex=2,Period=15,IsHistory=1,Ispandas=1")
                    if isinstance(stock_zh_df_tmp, c.EmQuantData) and stock_zh_df_tmp.ErrorCode != 0:
                        logger.error(
                            "it is failed to get stock data for {} {} and error code is {} error message is {}".
                            format(symbol_i, exchange_group, stock_zh_df_tmp.ErrorCode, stock_zh_df_tmp.ErrorMsg))
                        if stock_zh_df_tmp.ErrorMsg.find('service error') != -1 or \
                                stock_zh_df_tmp.ErrorCode == 10002011 or \
                                stock_zh_df_tmp.ErrorCode == 10002010 or \
                                stock_zh_df_tmp.ErrorCode == 10002004:
                            append_value(retried, exchange_group, symbol_i)
                elif datasource == DataSource.EFINANCE:
                    freq = 15
                    stock_zh_df_tmp: pd.DataFrame = ef.stock.get_quote_history(
                        str(symbol_i), klt=freq,
                        beg=(datetime.datetime.today() - datetime.timedelta(days=0)).strftime("%Y%m%d"),
                        end=(datetime.datetime.today() - datetime.timedelta(days=0)).strftime("%Y%m%d"),
                        fqt=0)
                if isinstance(stock_zh_df_tmp, pd.DataFrame) and len(stock_zh_df_tmp) > 0:
                    update_database(exchange, symbol_i, stock_zh_df_tmp, datasource)
                    if datasource == DataSource.AK_SHARE:
                        # watchdog for fetching stock data
                        global queue_history_data
                        queue_history_data.put(((group, str(symbol_i)), evt))
                        evt.wait()
                    pbar.update(1)
                    pbar.set_description_str(f'{exchange} Processing => {symbol_i}')
        elif type_func == 5:
            for symbol_i in symbols:
                dataframe_context: pd.DataFrame = context.data15mins[exchange].get(symbol_i)
                df_today = pd.DataFrame(columns=['gid', 'open', 'close', 'high', 'low', 'volume'])
                index_list = dataframe_context.index.tolist()
                total_len = 16
                i = - total_len
                while i < 0:
                    row = dataframe_context.iloc[i]
                    df_today.loc[index_list[i]] = [str(symbol_i), row['open'], row['close'],
                                                 row['high'], row['low'], row['volume']]
                    i += 1
                update_database(exchange, symbol_i, df_today, datasource)
        # EM has been obsoleted.
        elif type_func == 2:
            symbol_internals = []
            for symbol_i in symbols:
                symbol_internals.append(".".join([str(symbol_i), group]))
            if group == "SZ":
                market = "CNSESZ"
            else:
                market = "CNSESH"
            stock_zh_df_tmp = c.csd(symbol_internals, "OPEN,HIGH,LOW,CLOSE,VOLUME,TIME",
                                    (datetime.datetime.today() - datetime.timedelta(days=3)).strftime("%Y-%m-%d"),
                                    datetime.datetime.today().strftime("%Y-%m-%d"),
                                    "AdjustFlag=1,RowIndex=1,Period=1,Ispandas=1,Market=%s" % market)
            if isinstance(stock_zh_df_tmp, c.EmQuantData) and stock_zh_df_tmp.ErrorCode != 0:
                logger.error(
                    "it is failed to get stock data for {} and error code is {} error message is {}".
                    format(exchange_group, stock_zh_df_tmp.ErrorCode, stock_zh_df_tmp.ErrorMsg))
            elif isinstance(stock_zh_df_tmp, pd.DataFrame):
                inserttab(exchange, "", stock_zh_df_tmp, datasource, period=240, type_func=type_func)
    elif DataContext.iscountryUS():
        for symbol_i in symbols:
            stock_us_df_tmp = yf.download(tickers=symbol_i, auto_adjust=True, period="10d", interval="15m")
            if isinstance(stock_us_df_tmp, pd.DataFrame):
                inserttab(exchange, symbol_i, stock_us_df_tmp, datasource)
                inserttab(exchange, symbol_i, stock_us_df_tmp, datasource, period=30)


def insertdata_continue(exchange: str, group: str, symbols: list, c_point: str, retried, datasource: DataSource,
                        period: str = '15', type_func=1, adjust: str = "qfq"):
    pos = (pd.Series(symbols) == c_point).argmax() + 1
    insertdata(exchange, group, symbols[pos:], retried, datasource, period, type_func)


def insertdata_with_snapshot(exchange:str, group:str, symbols:list, context: DataContext, datasource: DataSource):
    insertdata(exchange, group, symbols, {}, datasource, type_func=5, context=context)


def loaddatalocked(indicator: str, exchange: str, symbols: list, operation: int, type_func=1,
                   datasource=DataSource.AK_SHARE, c_point='', retried={}, period=15,
                   context: DataContext=None):
    group = stock_group[indicator]
    if operation == 1:
        createtable(symbols, group, period)
    elif operation == 2:
        insertdata(group, exchange, symbols, retried, datasource, "%d" % period, type_func)
    elif operation == 3:
        insertdata_continue(group, exchange, symbols, c_point, retried, datasource, "%d" % period, type_func)
    elif operation == 4:
        droptable(symbols, group)
    elif operation == 5:
        insertdata_with_snapshot(group, exchange, symbols, context, datasource)


def normalizeticker(symbols: pd.Series) -> pd.Series:
    symbols_dict = {}
    dict_count = 0
    for ticker in symbols:
        ticker_str = str(ticker)
        diff = 6 - len(ticker_str)
        if diff > 0:
            prefix = ''
            count = 0
            while count < diff:
                prefix += '0'
                count += 1
        new_ticker = prefix + ticker_str
        symbols_dict[dict_count] = new_ticker
        dict_count += 1
    new_symbols = pd.Series(symbols_dict)
    return new_symbols


def selectgroup(indicator: str):
    symbol_path = symbol_paths[stock_group[indicator]]
    if pathlib.Path(symbol_path).is_file():
        symbolsfromcsv = pd.read_csv(symbol_path)
    else:
        logger.error("The file {} doesn't exist".format(symbol_path))
        exit()
    if DataContext.iscountryChina():
        if indicator in {"中小企业板", "创业板", "主板"}:
            header = "公司代码"
            group = 'SZ'
            if indicator in {"中小企业板", "主板"}:
                returndata = normalizeticker(symbolsfromcsv[header]).tolist()
            else:
                returndata = symbolsfromcsv[header].tolist()
        if indicator in {"科创板", "主板A股"}:
            header = 'SECURITY_CODE_A'
            group = 'SH'
            returndata = symbolsfromcsv[header].tolist()
    elif DataContext.iscountryUS():
        if indicator == "NASDAQ":
            group = 'O'
        elif indicator == "NYSE":
            group = 'N'
        elif indicator == "AMEX":
            group = 'A'
        symbol_group = symbolsfromcsv['SECURITY_CODE_A'].tolist()
        returndata = [symbol_us.split('.')[0] for symbol_us in symbol_group if len(symbol_us.split('.')) > 1]
    return group, returndata


def loaddata(indicators, operation: int, c_point='', datasource: DataSource = DataSource.AK_SHARE, period=15, type_func=1, isloginAlready=False):
    retriedStocks = {}
    if datasource == DataSource.EAST_MONEY and not isloginAlready:
        login_em()
    try:
        loaddatainternal(indicators, operation, type_func, c_point, retriedStocks, datasource, period)
        if datasource == DataSource.EAST_MONEY and type_func == 1:
            reloaddata(retriedStocks)
    finally:
        if not isloginAlready:
            if datasource == DataSource.EAST_MONEY:
                logout_em()
            if getdbconn():
                getdbconn().cursor().close()
                getdbconn().close()
            logger.debug("PostgreSQL connection is closed")



def loaddatainternal(indicators, operation: int, type_func=1, c_point='', retried={},
                     datasource: DataSource = DataSource.AK_SHARE, period=15, context: DataContext=None):
    try:
        for indicator in indicators:
            logger.debug("The board data is downloaded for is {} and native thread id is {} and thread ident is {}".
                         format(indicator, threading.get_native_id(), threading.get_ident()))
            group, symbols = selectgroup(indicator)
            loaddatalocked(indicator, group, symbols, operation, type_func, datasource,
                           c_point, retried, period, context)
    except psycopg2.Error as error:
        logger.error("Error while connecting to PostgreSQL", error)
    except Exception as ee:
        logger.error("error >>>", ee)
        traceback.print_exc()


def reloaddata(stocks, datasource: DataSource = DataSource.EAST_MONEY):
    if len(stocks) == 0:
        return
    else:
        retriedstocks = {}
        logger.debug("stocks reloaded are {}".format(stocks))
        for index, value in stocks.items():
            idx = index.split(",")
            if isinstance(value, list):
                symbols = value
            else:
                symbols = [value]
            if len(idx) > 1:
                insertdata(idx[0], idx[1], symbols, retriedstocks, datasource)
                logger.debug("Stocks that is still NOT downloaded are {}".format(retriedstocks))
            else:
                logger.debug("error occurs in idx -> {}".format(idx))


def checksymbols(*indicators: str):
    logger.info("start to check if there are new stocks on market.")
    for indicator in indicators:
        # TODO need to use old version of function selectgroup
        group_mark, symbols = selectgroup(indicator)
        print(symbols)
        symbol_path = symbol_paths[stock_group[indicator]]
        if pathlib.Path(symbol_path).is_file():
            symbolsfromcsv = pd.read_csv(symbol_path)
            length_symbols = len(symbols)
            diff = length_symbols - len(symbolsfromcsv)
            if diff > 0:
                for num in range(diff):
                    index = length_symbols - num - 1
                    print(symbols[index])
                    # create new partition and insert data
        else:
            symbols.to_csv(symbol_path, index=False)
    logger.info("Checking new stocks is done.")


class ProcessStatus(enum.Enum):
    STOP = 0
    START = 1


# ================================================================


class ActionBase:
    """
       This is a base class. All action should extend to it.
    """

    def __init__(self, data: pd.DataFrame):
        self._data = data

    # retrieve latest close price with a given period
    def close_ticker(self, period: int):
        return self._data['close'][period]

    # retrieve latest open price with a given period
    def open_ticker(self, period: int):
        return self._data['open'][period]

    # retrieve latest exchange volume with a given period
    def volume_ticker(self, period: int):
        return self._data['volume'][period]

    # retrieve latest high price with a given period
    def high_ticker(self, period: int):
        return self._data['high'][period]

    # retrieve latest low price with a given period
    def low_ticker(self, period: int):
        return self._data['low'][period]

    # retrieve quote with given period
    def refer_ticker(self, start_index: int, period: int, fn_ticker):
        index = start_index - period
        return fn_ticker(index)

    def getindex(self, timestamp: pd.Timestamp):
        tsi = self._data.index
        length = len(tsi)
        i = length - 1
        while i > -1:
            if tsi[i] == timestamp:
                break
            i -= 1
        else:
            i = None
        return i

    @abstractmethod
    def executeaction(self, **kwargs):
        pass


class MAAction(ActionBase):

    def __init__(self, startindex: int, endindex: int, period: int, data: pd.DataFrame):
        super().__init__(data)
        self.__startindex = startindex
        self.__endindex = endindex
        self.__period = period

    def __ma(self, fn_ticker):
        ret = {}
        totalinperiod: float64 = 0
        traverseback = 1 - self.__period
        if (len(self._data) + self.__endindex) < (0 - traverseback) or \
                self.__endindex > self.__startindex:
            return False, ret
        traversalindex = self.__endindex + traverseback
        i = traversalindex
        if self.__period > 1:
            while i < self.__endindex:
                totalinperiod += fn_ticker(i)
                i += 1

        outindex = self.__endindex

        def calc():
            nonlocal i, totalinperiod, traversalindex, outindex
            totalinperiod += fn_ticker(i)
            i += 1
            tmptotal = totalinperiod
            totalinperiod -= fn_ticker(traversalindex)
            traversalindex += 1
            ret[outindex] = tmptotal / self.__period
            outindex += 1

        calc()
        while i <= self.__startindex:
            calc()

        return True, ret

    def executeaction(self, **kwargs):
        return self.__ma(kwargs['fn_ticker'])


class CROSSUpMAAction(ActionBase):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)

    def __comparevalue(self, index: int, averagevalue: float64, period: int):
        distance = - period # distance should be negative
        open_cur = self.open_ticker(index)
        close_cur = self.close_ticker(index)
        # need to consider critical
        guard = len(self._data) + distance - 2
        if guard < 0:
            close_pre_period = close_pre = close_cur
        else:
            close_pre_period = self.refer_ticker(index, - distance, self.close_ticker)
            close_pre = self.refer_ticker(index, 1, self.close_ticker)
        sma_pre = (averagevalue * period - close_cur + close_pre_period) / period
        con_1 = close_cur >= averagevalue >= open_cur
        con_2 = close_pre < sma_pre and open_cur >= averagevalue and close_cur >= averagevalue
        return con_1 or con_2

    def executeaction(self, **kwargs):
        index_s = kwargs['startindex']
        index_e = kwargs['endindex']
        _cross_period = kwargs['cross_period']
        _greater_period = kwargs['greater_period']
        ret = pd.DataFrame(columns=columns)
        ma_cross = MAAction(index_s, index_e, _cross_period, self._data)
        valid_cross, result_cross = ma_cross.executeaction(fn_ticker=self.close_ticker)
        sma_greater = MAAction(index_s, index_e, _greater_period, self._data)
        valid_greater, result_greater = sma_greater.executeaction(fn_ticker=self.volume_ticker)
        if valid_cross:
            for index_cross, average_cross in result_cross.items():
                if self.__comparevalue(index_cross, average_cross, _cross_period):
                    row = self._data.loc[self._data.index[index_cross]]
                    ret.loc[len(ret)] = [row['gid'], row['open'], row['close'],
                                         row['high'], row['low'], row['volume'],
                                         row.name, False]
                    if valid_greater and \
                            index_cross in result_greater and \
                            self.volume_ticker(index_cross) > result_greater[index_cross]:
                        ret.loc[ret.index[-1]] = [row['gid'], row['open'], row['close'],
                                                  row['high'], row['low'], row['volume'],
                                                  row.name, True]
            return True, ret
        else:
            return False, ret


class XMAAction(ActionBase):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)

    def executeaction(self, **kwargs):
        def calc_xma(index):
            if index < minlength - 1:
                logger.error("length of data must be greater than {} for {} in {}}".
                             format(minlength, reason, operationtype))
                return False, values

            valid_r, value_r = target(index)
            intret = False
            if valid_r:
                if index == minlength - 1:
                    values[index] = weight * (value_r - intvalue) + intvalue
                    intret = True
                else:
                    index_p = index - 1
                    intret = calc_xma(index_p)
                    if intret:
                        values[index] = weight * (value_r - values[index_p]) + values[index_p]
            return valid_r and intret
        minlength = kwargs['minlength']
        target = kwargs['fnf']
        weight = kwargs['weight']
        intvalue = kwargs['intvalue']
        reason = kwargs.get('reason', "")
        operationtype = kwargs.get('operationtype', "")
        values = [None for i in range(len(self._data.index))]
        ret_v = calc_xma(len(self._data.index) - 1)
        return ret_v, values


class MACDAction(ActionBase):
    def __init__(self, data: pd.DataFrame, short_period, long_period, m_period):
        super().__init__(data)
        self.__short_period = short_period
        self.__long_period = long_period
        self.__m_period = m_period
        self.__data_length = len(self._data.index)
        self.__min_length = 1
        self.__dif_long_v = [None for i in range(self.__data_length)]
        self.__dif_short_v = [None for i in range(self.__data_length)]
        self.__dif_v = [None for i in range(self.__data_length)]
        self.__dea_v = [None for i in range(self.__data_length)]
        self.__xma = XMAAction(data)

    def __getCloseTicker(self, index):
        valid = False
        ret = None
        if index < self.__min_length - 1:
            logger.error("index is invalid in data for MACD", index)
        else:
            ret = self.close_ticker(index)
            if ret is not None:
                valid = True
        return valid, ret

    def __getdifvalue(self, index):
        valid = False
        ret = None
        if index < self.__min_length - 1:
            logger.error("index is invalid in data for MACD", index)
        else:
            ret = self.__dif_v[index]
            if ret is not None:
                valid = True
        return valid, ret

    def executeaction(self, **kwargs):
        ret_v, self.__dif_long_v = self.__xma.executeaction(minlength=self.__min_length,
                                                            fnf=self.__getCloseTicker,
                                                            weight=2 / (self.__long_period + 1),
                                                            intvalue=0,
                                                            reason='MACD_dif_long',
                                                            operationtype='EMA')
        if ret_v:
            ret_v, self.__dif_short_v = self.__xma.executeaction(minlength=self.__min_length,
                                                                   fnf=self.__getCloseTicker,
                                                                   weight=2 / (self.__short_period + 1),
                                                                   intvalue=0,
                                                                   reason='MACD_dif_short',
                                                                   operationtype='EMA')
            if ret_v:
                index = 0
                while index < self.__data_length:
                    if self.__dif_short_v[index] is not None and self.__dif_long_v[index] is not None:
                        self.__dif_v[index] = self.__dif_short_v[index] - self.__dif_long_v[index]
                    index += 1

                ret_v, self.__dea_v = self.__xma.executeaction(minlength=self.__min_length,
                                                                     fnf=self.__getdifvalue,
                                                                     weight=2 / (self.__m_period + 1),
                                                                     intvalue=0,
                                                                     reason='MACD_dea',
                                                                     operationtype='EMA')
        return ret_v, self.__dif_v, self.__dea_v


class StrategyBasedonMACDAction(ActionBase):
    def __init__(self, data: pd.DataFrame, period, short_period=12, long_period=26, m_period=9):
        super().__init__(data)
        self.__period = period
        self.__macd = MACDAction(data, short_period, long_period, m_period)
        self.__ret_v, self.__dif_v, self.__dea_v = self.__macd.executeaction()

    def executeaction(self, **kwargs):
        operation = kwargs.get('operation', '')
        ret_valid = False
        ret_valid_int = True
        ret_value = pd.DataFrame(columns=columns)
        data_length = len(self._data.index)
        if self.__ret_v and data_length >= self.__period:
            ret_valid = True
            if operation == 'strict':
                count = 0
                cursor = data_length - 1
                while count < self.__period:
                    dif_c = self.__dif_v[cursor]
                    dea_c = self.__dea_v[cursor]
                    if dif_c is None or dea_c is None:
                        ret_valid = False
                        ret_valid_int = False
                        break
                    else:
                        if dif_c > dea_c:
                            if count < self.__period - 1:
                                dif_p = self.__dif_v[cursor - 1]
                                dea_p = self.__dea_v[cursor - 1]
                                if dif_p is None or dea_p is None:
                                    ret_valid = False
                                    ret_valid_int = False
                                    break
                                elif dif_c <= dif_p or dea_c <= dea_p:
                                    ret_valid_int = False
                                    break
                        else:
                            ret_valid_int = False
                            break
                    count += 1
                    cursor -= 1
            elif operation == 'dif':
                count = 0
                cursor = data_length - 1
                while count < self.__period:
                    dif_c = self.__dif_v[cursor]
                    if dif_c is None:
                        ret_valid = False
                        ret_valid_int = False
                        break
                    else:
                        if count < self.__period - 1:
                            dif_p = self.__dif_v[cursor - 1]
                            if dif_p is None:
                                ret_valid = False
                                ret_valid_int = False
                                break
                            elif dif_c <= dif_p:
                                ret_valid_int = False
                                break
                    count += 1
                    cursor -= 1
            elif operation == 'cross_up':
                try:
                    diff, dea, macd = MACD(self._data.close.values)
                    tmp_ret = CROSS(diff, dea)
                    if tmp_ret[-2] or not tmp_ret[-1]:
                        ret_valid_int = False
                except BaseException as be:
                    logger.error("MACD is failed", be)
                    ret_valid = False
                    ret_valid_int = False

            else:
                ret_valid = False
                ret_valid_int = False
                logger.error("operation is not suppored by MACDGOUP", operation)

            if ret_valid_int:
                time_stamp = self._data.index[-1]
                row = self._data.loc[time_stamp]
                ret_value.loc[len(ret_value)] = [row['gid'], row['open'], row['close'],
                                                 row['high'], row['low'], row['volume'],
                                                 time_stamp, True]

        return ret_valid, ret_value


class LLVAction(ActionBase):
    def __init__(self, data: pd.DataFrame, rsv_period):
        super().__init__(data)
        self.__period = rsv_period

    def executeaction(self, **kwargs):
        ret_value = 0
        function = kwargs['fn_ticker']
        index = kwargs['index_c']
        if index - (self.__period - 1) < 0:
            return False, ret_value
        for i in range(self.__period):
            index_internal = index - i
            price = function(index_internal)
            if i == 0:
                ret_value = price
            elif price < ret_value:
                ret_value = price
        return True, ret_value


class HHVAction(ActionBase):
    def __init__(self, data: pd.DataFrame, rsv_period):
        super().__init__(data)
        self.__period = rsv_period

    def executeaction(self, **kwargs):
        ret_value = 0
        function = kwargs['fn_ticker']
        index = kwargs['index_c']
        if index - (self.__period - 1) < 0:
            return False, ret_value
        for i in range(self.__period):
            index_internal = index - i
            price = function(index_internal)
            if i == 0:
                ret_value = price
            elif price > ret_value:
                ret_value = price
        return True, ret_value


class KDAction(ActionBase):
    def __init__(self, data: pd.DataFrame, rsvperiod, kperiod, dperiod):
        super().__init__(data)
        self.__rsv_period = rsvperiod
        self.__k_period = kperiod
        self.__d_period = dperiod
        self.__llvaction = LLVAction(self._data, self.__rsv_period)
        self.__hhvaction = HHVAction(self._data, self.__rsv_period)
        self.__k_v = [None for i in range(len(self._data.index))]
        self.__d_v = [None for i in range(len(self._data.index))]
        self.__xma = XMAAction(data)

    def __rsv(self, index):
        ret = 0
        valid1, result_llv = self.__llvaction.executeaction(fn_ticker=self.low_ticker, index_c=index)
        if not valid1:
            return valid1, ret
        valid2, result_hhv = self.__hhvaction.executeaction(fn_ticker=self.high_ticker, index_c=index)
        if not valid2:
            return valid2, ret
        ret = (self.close_ticker(index) - result_llv) / (result_hhv - result_llv) * 100
        return True, ret

    def __kvalue(self, index):
        valid = False
        ret = None
        if index < self.__rsv_period - 1 or index >= len(self.__k_v):
            logger.error("index is invalid in kvalue", index)
        else:
            ret = self.__k_v[index]
            if ret is not None:
                valid = True
        return valid, ret

    '''
    def sma(self, fnf, n, m, index, values):
        if index < self.__rsv_period - 1:
            logger.error("index must be greater than %d for KD in sma", self.__rsv_period - 1)
            return False
        valid_r, value_r = fnf(index)
        k_t_v = False
        if valid_r:
            if index == self.__rsv_period - 1:
                values[index] = (value_r*m + 50*(n-m))/n
                k_t_v = True
            else:
                index_p = index - 1
                k_t_v = self.sma(fnf, n, m, index_p, values)
                if k_t_v:
                    values[index] = (value_r*m + values[index_p]*(n-m))/n
        return valid_r and k_t_v
    '''

    def executeaction(self, **kwargs):
        ret_v, self.__k_v = self.__xma.executeaction(minlength=self.__rsv_period,
                                                     fnf=self.__rsv,
                                                     weight=1/self.__k_period,
                                                     intvalue=50,
                                                     reason='KD_k',
                                                     operationtype='SMA')
        if ret_v:
            ret_v, self.__d_v = self.__xma.executeaction(minlength=self.__rsv_period,
                                                           fnf=self.__kvalue,
                                                           weight=1/self.__d_period,
                                                           intvalue=50,
                                                           reason='KD_d',
                                                           operationtype='SMA')
        return ret_v, self.__k_v, self.__d_v


class StrategyBasedOnKDAction(ActionBase):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)

    def crossupaction(self, time_stamp, k_v, d_v, c_v, is_twice_crossup_supported=False, cross_twice_period=0):
        index_c = self.getindex(time_stamp)
        if index_c is None:
            return False
        length = len(self._data.index)
        index_p = index_c - 1
        if index_c >= length or index_c < 0 or index_p >= length or index_p < 0:
            return False
        
        ret = False
        cross_v = CROSS(k_v, d_v)
        k_c_v = k_v[index_c]
        d_c_v = d_v[index_c]
        k_p_v = k_v[index_p]
        d_p_v = d_v[index_p]
        if not cross_v[index_p] and cross_v[index_c]:
            if c_v[0]:
                if not (k_c_v > c_v[1] and k_p_v > c_v[1]) and not (d_c_v > c_v[1] and d_p_v > c_v[1]):
                    ret = True
            else:
                ret = True
            if is_twice_crossup_supported and ret:
                i = index_p - 1
                while i > -1:
                    index_pre = i - 1
                    if index_pre > -1 and not cross_v[index_pre] and cross_v[i] and \
                            index_c - index_pre < cross_twice_period - 1:
                        break
                    i -= 2
                else:
                    ret = False
        return ret


    def entangleaction(self, time_stamp, periods, k_v, d_v, c_v):
        def comparevalue():
            if len(kd_results) < periods:
                return False
            ret = True
            for ii in range(periods):
                k_v_ii = kd_results[ii][0]
                d_v_ii = kd_results[ii][1]
                ret &= self.__compare_entanglement(k_v_ii, d_v_ii, 10)
                if not ret:
                    break
            return ret

        kd_results = []
        index_c = self.getindex(time_stamp)
        if index_c is None:
            return False
        for i in range(periods):
            index_t = index_c - i
            if index_t < 0:
                break
            k_v_i = k_v[index_t]
            d_v_i = d_v[index_t]
            if k_v_i is not None and d_v_i is not None:
                if c_v[0]:
                    if k_v_i <= c_v[1] and d_v_i <= c_v[1]:
                        kd_results.append((k_v_i, d_v_i))
                    else:
                        break
                else:
                    kd_results.append((k_v_i, d_v_i))
            else:
                break

        return comparevalue()

    def crossup_entangle_action(self, time_stamp, periods, k_v, d_v, c_v):
        if self.crossupaction(time_stamp, k_v, d_v, (False, 0)):
            index_c = self.getindex(time_stamp)
            if index_c is None:
                return False
            k_v_c = k_v[index_c]
            d_v_c = d_v[index_c]
            if self.__compare_entanglement(k_v_c, d_v_c, 10):
                return self.entangleaction(time_stamp, periods, k_v, d_v, c_v)
            else:
                index_p = index_c - 1
                if index_p < periods - 1:
                    return False
                time_stamp_p = self._data.index[index_p]
                return self.entangleaction(time_stamp_p, periods, k_v, d_v, c_v)

        return False

    def crossup_entangle_period_action(self, time_stamp, periods, duration, k_v, d_v):
        if self.crossupaction(time_stamp, k_v, d_v, (False, 0)):
            if self.crossup_entangle_action(time_stamp, periods, k_v, d_v, (False, 0)):
                return True
            index_c = self.getindex(time_stamp)
            if index_c is None:
                return False
            index_s = index_c - 2
            if index_s < periods:
                return False
            count = 1
            index_i = index_s - 1
            while index_i > -1:
                if count > duration - 2:
                    break
                time_stamp_i = self._data.index[index_i]
                #TODO: try crossup_entangle_action out
                if self.crossupaction(time_stamp_i, k_v, d_v, (False, 0)):
                    break
                if self.entangleaction(time_stamp_i, periods, k_v, d_v, (False, 0)):
                    return True
                index_i -= 1
                count += 1
            return False

    def entangle_period_action(self, time_stamp, periods, duration, k_v, d_v):
        index_c = self.getindex(time_stamp)
        if index_c is None:
            return False
        index_s = index_c - 2
        if index_s < periods:
            return False
        count = 1
        index_i = index_s - 1
        while index_i > -1:
            if count > duration - 2:
                break
            time_stamp_i = self._data.index[index_i]
            if self.entangleaction(time_stamp_i, periods, k_v, d_v, (False, 0)):
                return True
            index_i -= 1
            count += 1
        return False

    def deviate_price_k_action(self, time_stamp, duration, k_values):
        index_c = self.getindex(time_stamp)
        if index_c is None:
            return False
        length = min(index_c + 1, duration)
        count = 1
        while count < length:
            index = index_c - count
            if self.close_ticker(index_c) < self.close_ticker(index) and \
                    k_values[index_c] > k_values[index]:
                return True
            count += 1
        return False

    def deviate_price_k_s_action(self, time_stamp, duration, k_values, d_values):
        def GeneralLine(x1, y1, x2, y2):
            # general formula: Ax+By+C=0
            A = y2 - y1
            B = x1 - x2
            C = x2 * y1 - x1 * y2
            return A, B, C

        def cal_Intersection_Lines(line1, line2):
            A1, B1, C1 = GeneralLine(*line1)
            A2, B2, C2 = GeneralLine(*line2)
            D = A1 * B2 - A2 * B1
            if D == 0:
                return None
            else:
                x = (B1 * C2 - B2 * C1) / D
                y = (A2 * C1 - A1 * C2) / D
            return x, y

        index_c = self.getindex(time_stamp)
        if index_c is None:
            return False
        if self.close_ticker(index_c) == 0 and self.open_ticker(index_c) == 0:
            return False

        tmp_time = datetime.time(hour=time_stamp.hour, minute=time_stamp.minute)
        k_v = k_values
        d_v = d_values
        l_v = self._data.low.values
        if DataContext.iscountryChina():
            if tmp_time == datetime.time(hour=10, minute=30):
                k_v = REF(k_v, 1)
                d_v = REF(d_v, 1)
                l_v = REF(l_v, 1)
            elif tmp_time == datetime.time(hour=11, minute=30):
                k_v = REF(k_v, 2)
                d_v = REF(d_v, 2)
                l_v = REF(l_v, 2)
            elif tmp_time == datetime.time(hour=14):
                k_v = REF(k_v, 3)
                d_v = REF(d_v, 3)
                l_v = REF(l_v, 3)
            elif tmp_time == datetime.time(hour=15):
                k_v = REF(k_v, 4)
                d_v = REF(d_v, 4)
                l_v = REF(l_v, 4)
            else:
                return False
        else:
            return False
        try:
            min_low_duration_v = LLV(l_v, duration)
        except BaseException as be:
            logger.error("LLV is failed", be)
            return False
        cross_v = CROSS(k_v, d_v)
        if len(k_v) < duration or len(d_v) < duration:
            return False
        count = -1
        while count > - duration:
            if not cross_v[count - 1] and cross_v[count]:
                break
            count -= 1
        else:
            return False
        cross_point = cal_Intersection_Lines([0, d_v[count - 1], 1, d_v[count]],
                                             [0, k_v[count - 1], 1, k_v[count]])
        if (min_low_duration_v[-1] - self.close_ticker(index_c)) > min_low_duration_v[-1] * 1 / 100 and \
           (k_values[index_c] - cross_point[1]) > cross_point[1] * 5 / 100:
            return True
        return False

    def __compare_entanglement(self, k_v_t, d_v_t, diff_v):
        return abs(k_v_t - d_v_t) <= diff_v

    def executeaction(self, **kwargs):
        def locdata():
            row = self._data.loc[time_stamp]
            ret_value.loc[len(ret_value)] = [row['gid'], row['open'], row['close'],
                                             row['high'], row['low'], row['volume'],
                                             time_stamp, True]

        occurrences = kwargs['occurrence_time']
        operation = kwargs['operation']
        c_v = kwargs.get('crossvalue', (False, 0))
        periods = kwargs.get('periods', 1)
        duration = kwargs.get('duration', 40)
        cross_twice_period = kwargs.get('cross_twice_period', 60)
        rsv_p = kwargs.get('rsv_period', 9)
        k_p = kwargs.get('k_period', 3)
        d_p = kwargs.get('d_period', 3)
        k_v_o = kwargs.get('KValues', None)
        d_v_o = kwargs.get('DValues', None)
        ret_valid = True
        ret_value = pd.DataFrame(columns=columns)
        if k_v_o is not None and d_v_o is not None:
            valid = True
            k_v = k_v_o
            d_v = d_v_o
        else:
            try:
                k_v, d_v, j_v = KDJ(self._data.close.values, self._data.high.values, self._data.low.values)
                valid = True
            except BaseException as be:
                logger.error("The KDJ is failed", be)
                valid = False
        if valid:
            for time_stamp in occurrences:
                if operation == 'cross_up':
                    if self.crossupaction(time_stamp, k_v, d_v, c_v):
                        locdata()
                elif operation == 'entangle':
                    if self.entangleaction(time_stamp, periods, k_v, d_v, c_v):
                        locdata()
                elif operation == 'entangle_and_cross_up':
                    if self.crossup_entangle_action(time_stamp, periods, k_v, d_v, c_v):
                        locdata()
                elif operation == 'entangle_and_cross_up_within_period':
                    if self.crossup_entangle_period_action(time_stamp, periods, duration, k_v, d_v):
                        locdata()
                elif operation == 'entangle_within_period':
                    if self.entangle_period_action(time_stamp, periods, duration, k_v, d_v):
                        locdata()
                elif operation == 'divergence_price_lower_and_k_higher':
                    if self.deviate_price_k_action(time_stamp, duration, k_v):
                        locdata()
                elif operation == 'divergence_price_lower_and_k_higher_simple':
                    if self.deviate_price_k_s_action(time_stamp, duration, k_v, d_v):
                        locdata()
                elif operation == 'cross_up_twice':
                    if self.crossupaction(time_stamp, k_v, d_v, c_v, True, cross_twice_period):
                        locdata()
                else:
                    logger.error("%s is not supported!" % operation)
        else:
            ret_valid = False

        return ret_valid, ret_value


class OBVAction(ActionBase):
    def __init__(self, data: pd.DataFrame, obv_period: int):
        super().__init__(data)
        self.__obv_p = obv_period

    def executeaction(self, **kwargs):
        index = kwargs['index']
        total_value = 0
        ret_valid = False
        if index - (self.__obv_p - 1) < 0:
            return ret_valid, total_value

        for i in range(self.__obv_p):
            index_internal = index - i
            price_dist = self.high_ticker(index_internal) - self.low_ticker(index_internal)
            if price_dist != 0:
                total_value += self.volume_ticker(index_internal) * \
                               ((self.close_ticker(index_internal)-self.low_ticker(index_internal))-
                                (self.high_ticker(index_internal)-self.close_ticker(index_internal)))/price_dist
        else:
            ret_valid = True

        return ret_valid, total_value


class OBVUpACTION(ActionBase):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)

    def __calcv_a_obv(self, index: int, period_a: int, period: int):
        ret_valid = False
        total_value = 0
        obv_indicator = OBVAction(self._data, period)
        if index - (period_a - 1) - (period - 1) < 0:
            return ret_valid, total_value
        for i in range(period_a):
            index_interal = index - i
            valid, obv_v = obv_indicator.executeaction(index=index_interal)
            if not valid:
                break
            total_value += obv_v
        else:
            total_value = total_value / period_a
            ret_valid = True

        return ret_valid, total_value

    def executeaction(self, **kwargs):
        occurrences = kwargs['occurrence_time']
        obv_p = kwargs['obv_period']
        obv_a_p = kwargs['obv_a_period']
        ret_valid = False
        ret_value = pd.DataFrame(columns=columns)
        obv_indicator = OBVAction(self._data, obv_p)
        oa = occurrences.array
        for time_stamp_original in oa:
            cur_index = self.getindex(time_stamp_original)
            if cur_index is None:
                continue
            valid1, obv_v = obv_indicator.executeaction(index=cur_index)
            if valid1:
                valid2, obv_a_v = self.__calcv_a_obv(cur_index, obv_a_p, obv_p)
                if valid2:
                    ret_valid = True
                    if obv_v > 0 and obv_v > obv_a_v:
                        row = self._data.loc[time_stamp_original]
                        ret_value.loc[len(ret_value)] = [row['gid'], row['open'], row['close'],
                                                         row['high'], row['low'], row['volume'],
                                                         time_stamp_original, True]

        return ret_valid, ret_value


class StrategyBasedOnDayKAction(ActionBase):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)
        self.__min_length = 1

    def __getCloseTicker(self, index):
        valid = False
        ret = None
        if index < self.__min_length - 1:
            logger.error("index is invalid in data for EXPEMA", index)
        else:
            ret = self.close_ticker(index)
            if ret is not None:
                valid = True
        return valid, ret

    def __calcamplitudeavg(self, period: int, percent: float):
        ret_valid_t = False
        ret_value_t = False
        requiredlength = period + 2
        if len(self._data) < requiredlength:
            return ret_valid_t, ret_value_t
        ret_valid_t = True
        total: float = 0
        for i in range(1, requiredlength-1):
            index: int = -1 - i
            total += (self.high_ticker(index)-self.low_ticker(index))/self.close_ticker(index-1)
        if total/period >= percent/100:
            ret_value_t = True
        return ret_valid_t, ret_value_t

    def __calc_price_kavg(self, k_period, isgreater):
        ret_valid_t = False
        ret_value_t = False
        if len(self._data.index) >= k_period:
            start_index = end_index = -1
            maaction = MAAction(start_index, end_index, k_period, self._data)
            valid_ma, result_ma = maaction.executeaction(fn_ticker=self.close_ticker)
            if valid_ma:
                ret_valid_t = True
                if start_index in result_ma:
                    tmp_ret = self.close_ticker(start_index) >= result_ma[start_index]
                    ret_value_t = not (isgreater ^ tmp_ret)

        return ret_valid_t, ret_value_t


    def __calckavg(self, k_period, calc_period, isgreater):
        ret_valid_t = False
        ret_value_t = True
        start_index = -1
        end_index = 0 - calc_period
        if len(self._data.index) >= k_period + calc_period - 1:
            maaction = MAAction(start_index, end_index, k_period, self._data)
            valid_ma, result_ma = maaction.executeaction(fn_ticker=self.close_ticker)
            if valid_ma:
                ret_valid_t = True

                if isgreater:
                    for index, avg_v in result_ma.items():
                        if self.close_ticker(index) <= avg_v:
                            ret_value_t = False
                            break
                if isgreater and not ret_value_t:
                    return ret_valid_t, ret_value_t

                count = 0
                cursor = start_index
                while count < calc_period - 1:
                    cursor_p = cursor - 1
                    if cursor in result_ma and cursor_p in result_ma:
                        if result_ma[cursor] <= result_ma[cursor_p]:
                            ret_value_t = False
                            break
                    else:
                        ret_value_t = False
                        break
                    count += 1
                    cursor -= 1
        return ret_valid_t, ret_value_t

    def __calcemadif(self, dif_period, calc_period):
        ret_valid_t = False
        ret_value_t = True
        start_index = -1
        ema = XMAAction(self._data)
        ret_v, ema_dif_v = ema.executeaction(minlength=1,
                                             fnf=self.__getCloseTicker,
                                             weight=2 / (dif_period + 1),
                                             intvalue=0,
                                             reason='EXPMA_dif',
                                             operationtype='EMA')
        if ret_v:
            ret_valid_t = True
            count = 0
            cursor = start_index
            while count < calc_period - 1:
                cursor_p = cursor - 1
                if ema_dif_v[cursor] is not None and ema_dif_v[cursor_p] is not None:
                    if ema_dif_v[cursor] <= ema_dif_v[cursor_p]:
                        ret_value_t = False
                        break
                else:
                    ret_value_t = False
                    break
                count += 1
                cursor -= 1
        return ret_valid_t, ret_value_t


    def executeaction(self, **kwargs):
        operation = kwargs['operation']
        amplitude_period = kwargs.get('amplitude_peroid', 5)
        amplitude_percent = kwargs.get('amplitude_percent', 3)
        avgk_k_period = kwargs.get('avgk_period', 20)
        avgk_calc_period = kwargs.get('avgk_calc_period', 2)
        avgk_greater = kwargs.get('avgk_greater', False)
        avg_ema_dif_period = kwargs.get('avg_ema_dif_period', 12)
        avg_ema_avg_period = kwargs.get('avg_ema_dif_period', 50)
        ret_valid = False
        ret_value_bool = False
        ret_value = pd.DataFrame(columns=columns)
        if operation == 'amplitude_avg':
            ret_valid, ret_value_bool = self.__calcamplitudeavg(amplitude_period, amplitude_percent)
        elif operation == 'avg_k_go':
            ret_valid, ret_value_bool = self.__calckavg(avgk_k_period, avgk_calc_period, avgk_greater)
        elif operation == 'price_k_avg':
            ret_valid, ret_value_bool = self.__calc_price_kavg(avgk_k_period, avgk_greater)
        elif operation == 'expma_dif_go':
            ret_valid, ret_value_bool = self.__calcemadif(avg_ema_dif_period, avgk_calc_period)
        else:
            logger.error("%s is not supported!" % operation)

        if ret_valid and ret_value_bool:
            time_stamp = self._data.index[-1]
            row = self._data.loc[time_stamp]
            ret_value.loc[len(ret_value)] = [row['gid'], row['open'], row['close'],
                                             row['high'], row['low'], row['volume'],
                                             time_stamp, True]
        return ret_valid, ret_value


class EXPMACrossAction(ActionBase):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)

    def executeaction(self, **kwargs):
        ret_valid = True
        ret_value = pd.DataFrame(columns=columns)
        if len(self._data.close) < 2:
            ret_valid = False
            return ret_valid, ret_value
        try:
            expma_12, expma_50 = EXPMA(self._data.close.values)
            tmp_ret = CROSS(expma_12, expma_50)
            if tmp_ret[-2] == 0 and tmp_ret[-1] == 1:
                time_stamp = self._data.index[-1]
                row = self._data.loc[time_stamp]
                ret_value.loc[len(ret_value)] = [row['gid'], row['open'], row['close'],
                                                 row['high'], row['low'], row['volume'],
                                                 time_stamp, True]
        except BaseException as be:
            logger.error("The EXPMA is failed", be)
            ret_valid = False

        return ret_valid, ret_value


class StockData:

    def __init__(self, sector: str = ''):
        self.sector = sector
        self.__data = {}

    def update(self, symbol: str, data: pd.DataFrame):
        self.__data.update({str(symbol): data})

    def get(self, symbol: str) -> pd.DataFrame:
        try:
            ret = self.__data[str(symbol)]
        except Exception as ee:
            logger.error("error >>>", ee)
            traceback.print_exc()
            ret = pd.DataFrame(columns=columns)
        return ret

    def has_symbol(self, symbol: str) -> bool:
        if symbol in self.__data:
            return True
        else:
            return False

    def keys(self):
        return self.__data.keys()

    def clear(self):
        self.__data.clear()

    def remove(self, symbol: str) -> pd.DataFrame:
        try:
            if symbol in self.__data:
                del self.__data[symbol]
        except Exception as ee:
            logger.error("error >>>", ee)
            traceback.print_exc()


def loadsectors(context: DataContext):
    if not DataContext.iscountryChina():
        return
    filename = "sectors_allocation"
    filepath = os.path.join(r'./', filename)
    append_value(context.sectors, '000001', [str(code).zfill(6) for code in DataContext.code_spotlighted])
    with open(filepath, 'r') as file:
        for line in file.read().splitlines():
            sector_symbols = line.split(":")
            if len(sector_symbols) > 1:
                symbols = sector_symbols[1].split(",")
                if len(symbols) > 1:
                    for symbol in symbols:
                        append_value(context.sectors, sector_symbols[0], symbol)


def loadsectorsfromEM():
    date_t = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
    if DataContext.iscountryChina():
        sectors = sectors_CN.keys()
    elif DataContext.iscountryUS():
        sectors = sectors_US.keys()
    filename = "sectors_allocation"
    filepath = os.path.join(r'./', filename)
    with open(filepath, 'w+') as file:
        for sector_i in sectors:
            if sector_i == '000001':
                pass
            else:
                data = c.sector(sector_i, date_t)
                if data.ErrorCode != 0:
                    logger.debug("request sector %s Error, %s" % (sector_i, data.ErrorMsg))
                else:
                    file.write('{}:'.format(sector_i))
                    symbolsinsector = []
                    for code in data.Data:
                        code_l = code.split(".")
                        if len(code_l) > 1:
                            symbolsinsector.append(code_l[0])
                    file.writelines(",".join(symbolsinsector))
                    file.write('\r\n')

fetchdatacounter = 0
barcounter_15 = 0
roundresult_15 = 0
firstroundresult_15 = 0
barcounter_30 = 0
roundresult_30 = 0
firstroundresult_30 = 0
barcounter_60 = 0
roundresult_60 = 0
firstroundresult_60 = 0
fetch_count = 1

def snapshot(context: DataContext):
    # 1) rank sectors over previous consecutive 10 business days
    # 2) start fetching data once market is open
    # 3) get a snapshot of stocks depending on country every other 3 seconds according to limitation
    # 4) update stock data in context
    # 5) calculate indicators based on newly fetched stock data
    # 6) send result to another thread to handle
    global fetchdatacounter, fetch_count
    global barcounter_15, roundresult_15, firstroundresult_15
    global barcounter_30, roundresult_30, firstroundresult_30
    global barcounter_60, roundresult_60, firstroundresult_60

    current_time = datetime.datetime.now()
    current_date = datetime.date.today()
    if DataContext.iscountryChina():
        opentime = datetime.datetime.combine(current_date, context.marketopentime)
        closetime = datetime.datetime.combine(current_date, context.marketclosetime)
        breakstarttime = datetime.datetime.combine(current_date, context.marketbreakstarttime)
        breakstoptime = datetime.datetime.combine(current_date, context.marketbreakstoptime)
    elif DataContext.iscountryUS():
        opentime = datetime.datetime.combine(current_date, context.marketopentime)
        closetime = datetime.datetime.combine(current_date, context.marketclosetime)

    target_time = datetime.timedelta(days=0, hours=0, minutes=0, seconds=0)

    symbols_exchange = []
    for sector in context.markets:
        symbols_exchange += context.symbols_exchange[stock_group[sector]]
    symbols_original_len = len(symbols_exchange)
    symbols_tmp = set(symbols_exchange)
    symbols_tmp.difference_update(DataContext.invalid_stock_codes)
    symbols_exchange = list(symbols_tmp)

    def update_stock_data_in_context(timeout=0):
        global fetchdatacounter, fetch_count
        global barcounter_15, roundresult_15, firstroundresult_15
        global barcounter_30, roundresult_30, firstroundresult_30
        global barcounter_60, roundresult_60, firstroundresult_60

        # 1) french data
        logger.debug("totally scan %d stocks but the number of original stock codes is %d" %
                     (len(symbols_exchange), symbols_original_len))
        '''
        EM has been obsoleted.
        stock_data = csqsnapshot_t(symbols_exchange, "NOW,VOLUME,OPEN,HIGH,LOW", "Ispandas=1")
        if not isinstance(stock_data, c.EmQuantData):
        '''
        logger.debug("Start to fetch stock data count: {}".format(fetch_count))
        starttime = time.perf_counter()
        stock_data = None
        try:
            # FIXME: only for datasource AKShare
            # stock_data = ak.stock_zh_a_spot()
            # need to set applied_data_source
            applied_data_source = DataSource.EFINANCE
            stock_data = ef.stock.get_realtime_quotes()
            stock_data.columns = [
                'CODE',
                '_',
                '_',
                'NOW',
                'HIGH',
                'LOW',
                'OPEN',
                '_',
                '_',
                '_',
                '_',
                'VOLUME',
                '_',
                '_',
                '_',
                '_',
                '_',
                '_'
            ]
            stock_data = stock_data[[
                'CODE',
                'NOW',
                'HIGH',
                'LOW',
                'OPEN',
                'VOLUME'
            ]]
            stock_data = stock_data.astype({
                'NOW': "float",
                'HIGH': "float",
                'LOW': "float",
                'OPEN': "float",
                'VOLUME': "float"
            })
        except:
            logger.error("It was failed to get stock data count: {}".format(fetch_count))
        endtime = time.perf_counter()
        logger.debug("time consumption of count {} is {}s".format(fetch_count, endtime - starttime))
        fetch_count += 1
        if stock_data is None or not isinstance(stock_data, pd.DataFrame):
            # FIXME: only for datasource AKShare
            # logger.debug("request ak.stock_zh_a_spot Error at {} ".format(current_time))
            logger.debug("request ef.stock.get_realtime_quotes Error at {} ".format(current_time))
            time.sleep(timeout)
            return False
        else:
            # 2) update stock data in context
            logger.debug("request real quote with success at {} ".format(current_time))
            fetchdatacounter += 1
            logger.debug("fetchdatacounter is %d" % fetchdatacounter)
            deltatime = current_time - opentime
            roundresult_15 = deltatime.seconds // (15 * 60)
            roundresult_30 = deltatime.seconds // (30 * 60)
            if DataContext.iscountryChina():
                if (current_time - opentime) >= target_time and (breakstarttime - current_time) >= target_time:
                    roundresult_60 = deltatime.seconds // (60 * 60)
                else:
                    tmp_round = (current_time - breakstoptime).seconds // (60 * 60)
                    if tmp_round == 0:
                        roundresult_60 = 3
                    else:
                        roundresult_60 = tmp_round + 3
            elif DataContext.iscountryUS():
                if target_time <= (closetime - current_time) <= datetime.timedelta(days=0, hours=0, minutes=30,
                                                                                   seconds=0):
                    roundresult_60 = 7
                else:
                    roundresult_60 = deltatime.seconds // (60 * 60)
            time_windows_15[roundresult_15] += 1
            time_windows_30[roundresult_30] += 1
            time_windows_60[roundresult_60] += 1
            if fetchdatacounter == 1:
                firstroundresult_15 = roundresult_15
                firstroundresult_30 = roundresult_30
                firstroundresult_60 = roundresult_60
            if time_windows_60[roundresult_60] == 1:
                barcounter_60 += 1
                logger.debug("The value of roundresult_60 is %d" % roundresult_60)
                logger.debug("The number of 60 mins bar is %d" % barcounter_60)
            if time_windows_30[roundresult_30] == 1:
                barcounter_30 += 1
                logger.debug("The value of roundresult_30 is %d" % roundresult_30)
                logger.debug("The number of 30 mins bar is %d" % barcounter_30)
            if time_windows_15[roundresult_15] == 1:
                barcounter_15 += 1
                logger.debug("The value of roundresult_15 is %d" % roundresult_15)
                logger.debug("The number of 15 mins bar is %d" % barcounter_15)
                # for the first time to update open value
                updatestockdata(stock_data, True, applied_data_source)
            else:
                updatestockdata(stock_data, datasource=applied_data_source)
            logger.debug("update stock data in context")
            return True

    def updatestockdata(stockdata: pd.DataFrame, isnewrow: bool = False, datasource = DataSource.EFINANCE):
        def getrecordtime(period: int):
            if (current_time - closetime) >= target_time:
                return datetime.datetime.combine(current_date, datetime.time(hour=15))
            elif (opentime - current_time) >= target_time:
                return datetime.datetime.combine(current_date - datetime.timedelta(days=1),
                                                 datetime.time(hour=15))
            elif (current_time - breakstarttime) >= target_time and (breakstoptime - current_time) >= target_time:
                return datetime.datetime.combine(current_date, datetime.time(hour=11, minute=30))
            if period == 15 or period == 30:
                slot = current_time.minute // period + 1
                if slot == 60 // period:
                    recordtime = datetime.datetime.combine(current_date, datetime.time(hour=current_time.hour + 1))
                else:
                    recordtime = datetime.datetime.combine(current_date, datetime.time(hour=current_time.hour, minute=slot * period))
            elif period == 60:
                if DataContext.iscountryChina():
                    if (current_time - opentime) >= target_time and (breakstarttime - current_time) >= target_time:
                        slot = (current_time - opentime).seconds // (period * 60) + 1
                        recordtime = datetime.datetime.combine(current_date, datetime.time(hour=opentime.hour + slot, minute=opentime.minute))
                    else:
                        slot = (current_time - breakstoptime).seconds // (period * 60) + 1
                        recordtime = datetime.datetime.combine(current_date, datetime.time(hour=breakstoptime.hour + slot))
                elif DataContext.iscountryUS():
                    if target_time <= (closetime - current_time) <= datetime.timedelta(days=0, hours=0, minutes=30,
                                                                                       seconds=0):
                        recordtime = datetime.datetime.combine(current_date, datetime.time(hour=closetime.hour))

                    else:
                        slot = (current_time - opentime).seconds // (period * 60) + 1
                        recordtime = datetime.datetime.combine(current_date, datetime.time(hour=opentime.hour + slot, minute=opentime.minute))

            return recordtime

        def sumvolume(size_p: int, start: int, dataset: pd.DataFrame):
            sum_volume = 0
            try:
                for j in range(size_p):
                    if start < 0:
                        sum_volume += dataset.iloc[start - j]['volume']
                    else:
                        sum_volume += dataset.iloc[start + j]['volume']
            except Exception as exce:
                # traceback.print_exc()
                logger.error("Symbol error occurred with {} error message is {}".format(dataset.iloc[-1]['gid'], exce))
            return sum_volume

        def updateexistingrow(firstk: bool, barcounter: int, dataset: pd.DataFrame, isdayk: bool=False):
            if isdayk:
                dataset.loc[dataset.index[-1]] = [symbol_idx, row['OPEN'], row['NOW'], row['HIGH'], row['LOW'], row['VOLUME']]
                return
            if firstk:
                volume_cur_i = row['VOLUME']
            else:
                sum_v_i = sumvolume(barcounter - 1, -2, dataset)
                volume_cur_i = row['VOLUME'] - sum_v_i
            open_tmp = dataset.iloc[-1]['open']
            cur_high = dataset.iloc[-1]['high']
            cur_low = dataset.iloc[-1]['low']
            if row['NOW'] > cur_high:
                cur_high = row['NOW']
            if row['NOW'] < cur_low:
                cur_low = row['NOW']
            dataset.loc[dataset.index[-1]] = [symbol_idx, open_tmp, row['NOW'], cur_high, cur_low, volume_cur_i]

        if fetchdatacounter == 1:
            isfirstK_240 = True
        else:
            isfirstK_240 = False

        if firstroundresult_60 == roundresult_60:
            isfirstK_60 = True
        else:
            isfirstK_60 = False

        if firstroundresult_30 == roundresult_30:
            isfirstK_30 = True
        else:
            isfirstK_30 = False

        if firstroundresult_15 == roundresult_15:
            isfirstk_15 = True
        else:
            isfirstk_15 = False

        round_number_15 = 0
        round_number_30 = 0
        round_number_60 = 0
        for tmp_number in time_windows_15:
            if tmp_number > 0:
                round_number_15 += 1
        for tmp_number in time_windows_30:
            if tmp_number > 0:
                round_number_30 += 1
        for tmp_number in time_windows_60:
            if tmp_number > 0:
                round_number_60 += 1

        for index in stockdata.index.array:
            for sector_usd in context.markets:
                # EM has been obsoleted.
                # index_s = str(index)
                index_s = stockdata['CODE'][index]
                # only for Chinese stock
                if datasource == DataSource.EFINANCE:
                    tmp_index_list = [index_s, exchanges[sector_usd]]
                else:
                    tmp_index_list = [index_s[2:], index_s[0:2]]
                index_s = ".".join(tmp_index_list)
                sector_code = stock_group[sector_usd]
                if index_s in context.symbols_exchange[sector_code]:
                    # FIXME: convert object or str to numeric for each missing data
                    # FIXME: dynamically adjust list of stock symbols in order to skip
                    #  stock that doesn't have data today
                    try:
                        if DataContext.iscountryChina():
                            symbol_idx = index_s[:-3]
                        elif DataContext.iscountryUS():
                            symbol_idx = index_s[:-2]
                        tmpdata = context.data15mins[sector_code].get(symbol_idx)
                        tmpdata_30 = context.data30mins[sector_code].get(symbol_idx)
                        tmpdata_60 = context.data60mins[sector_code].get(symbol_idx)
                        tmpdata_240 = context.data240mins[sector_code].get(symbol_idx)
                        row = stockdata.loc[index]
                        # create a new row based on the period of 15 mins and it represent the next period because it is the
                        # beginning of the next period so that it is needed to use next record time as index
                        if isnewrow:
                            record_time = getrecordtime(15)
                            if isfirstk_15:
                                volume_cur = row['VOLUME']
                            else:
                                sum_v = sumvolume(barcounter_15 - 1, -1, tmpdata)
                                volume_cur = row['VOLUME'] - sum_v
                            tmpdata.loc[pd.Timestamp(record_time)] = [symbol_idx, row['NOW'], row['NOW'], row['NOW'],
                                                                      row['NOW'], volume_cur]

                            record_time = getrecordtime(30)
                            if isfirstK_30:
                                tmpdata_30.loc[pd.Timestamp(record_time)] = [symbol_idx, row['NOW'], row['NOW'],
                                                                             row['NOW'], row['NOW'], row['VOLUME']]
                            else:
                                if (roundresult_15 % 2) == 0:
                                    sum_v = sumvolume(barcounter_30 - 1, -1, tmpdata_30)
                                    volume_cur = row['VOLUME'] - sum_v
                                    tmpdata_30.loc[pd.Timestamp(record_time)] = [symbol_idx, row['NOW'], row['NOW'],
                                                                                 row['NOW'], row['NOW'], volume_cur]
                                else:
                                    updateexistingrow(isfirstK_30, barcounter_30, tmpdata_30)

                            record_time = getrecordtime(60)
                            if isfirstK_60:
                                tmpdata_60.loc[pd.Timestamp(record_time)] = [symbol_idx, row['NOW'], row['NOW'],
                                                                             row['NOW'], row['NOW'], row['VOLUME']]
                            else:
                                if (roundresult_15 % 4) == 0:
                                    sum_v = sumvolume(barcounter_60 - 1, -1, tmpdata_60)
                                    volume_cur = row['VOLUME'] - sum_v
                                    tmpdata_60.loc[pd.Timestamp(record_time)] = [symbol_idx, row['NOW'], row['NOW'],
                                                                                 row['NOW'], row['NOW'], volume_cur]
                                else:
                                    updateexistingrow(isfirstK_60, barcounter_60, tmpdata_60)

                            if isfirstK_240:
                                tmpdata_240.loc[pd.Timestamp(current_date)] = [symbol_idx, row['OPEN'], row['NOW'],
                                                                               row['HIGH'],
                                                                               row['LOW'], row['VOLUME']]
                            else:
                                updateexistingrow(isfirstK_240, -1, tmpdata_240, True)

                        else:
                            updateexistingrow(isfirstk_15, barcounter_15, tmpdata)
                            updateexistingrow(isfirstK_30, barcounter_30, tmpdata_30)
                            updateexistingrow(isfirstK_60, barcounter_60, tmpdata_60)
                            updateexistingrow(isfirstK_240, -1, tmpdata_240, True)
                    except BaseException as be:
                        logger.debug("It is failed to update context data, symbol is {}".format(symbol_idx))
                        logger.error("It is failed to update context data", be)
                    break

    # FIXME because EM has been obsoleted.
    # calcrankofchange()
    while True:
        current_time = datetime.datetime.now()
        if DataContext.iscountryChina():
            timecondition = (((current_time - opentime) >= target_time and (breakstarttime - current_time) >= target_time)
                or ((current_time - breakstoptime) >= target_time and (closetime - current_time) >= target_time))
        elif DataContext.iscountryUS():
            timecondition = (((current_time - opentime) >= target_time) and ((closetime - current_time) >= target_time))
        
        if timecondition:
            if update_stock_data_in_context():
                # 3) calculate indicators
                logger.debug("run 12 strategies")
                try:
                    result = {current_time: quantstrategies(context)}
                except Exception as ee:
                    logger.error("It is failed to execute quantitative strategies. error >>>", ee)
                    traceback.print_exc()
                    result = {}
                else:
                    logger.info("execute quantitative strategies successfully.")
                context.queue.put(result)
                logger.debug("send result to another thread to handle and sleep")

                logger.debug("start to sleep with 720 seconds")
                # TODO: replace sleep() with threading.Timer()
                time.sleep(720)
                logger.debug("sleep is done with 720 seconds")
                # if closetime - current_time > datetime.timedelta(minutes=5):
                # else:
                #     logger.debug("start to sleep with 45 seconds")
                #     time.sleep(45)
                #     logger.debug("sleep is done with 45 seconds")
            '''
            EM has been obsoleted.   
            elif stock_data.ErrorCode != 0:
                logger.debug("Request csqsnapshot Error error code is {}; error message is {}; codes is {}".
                             format(stock_data.ErrorCode, stock_data.ErrorMsg, stock_data.Codes))
                if stock_data.ErrorCode == 10002008 or stock_data.ErrorMsg.find('timeout') != -1:
                    logger.debug("timeout occurred so sleep 180 seconds and then logout")
                    logout_em()
                    time.sleep(180)
                    logger.debug("login again after sleeping 180")
                    login_em()
                time.sleep(30)
                logger.debug("sleep of 30 is done due to error occurring during requesting csqsnapshot")
            '''
        elif (current_time - closetime) >= target_time:
            # FIXME: Only for datasource AKShare
            # if not update_stock_data_in_context():
            #    update_stock_data_in_context(1200)
            summarytotalresult(context)
            logger.debug("market is closed so that snapshot quits")
            context.queue.put(ProcessStatus.STOP)
            print("time windows for 15 mins:")
            print(time_windows_15)
            print("time windows for 30 mins:")
            print(time_windows_30)
            print("time windows for 60 mins:")
            print(time_windows_60)
            print("total number of fetching data is %d" % fetchdatacounter)
            break
        else:
            logger.debug("market is not open or break is ongoing so that await. Now is {}".format(current_time))
            time.sleep(10)


queue_history_data = Queue()
failed_to_get_data_symbols = []


class CompositeStrategyBase:
    def __init__(self, data_context: DataContext):
        self._data_context = data_context

    @abstractmethod
    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        pass


# 前五日价格振幅平均值大于等于3%     (填充strategy100由merge函数实现)
class Strategy100(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
        
    def execute_action(self, stock_sector, stock_symbol, result=None) -> bool:
        transientresult100 = self._data_context.totalresult[DataContext.strategy100]
    
        if not DataContext.is_first_time_to_run:
            if stock_symbol not in transientresult100:
                return False
        else:
            dataset_240 = self._data_context.data240mins[stock_sector].get(stock_symbol)
            if len(dataset_240) == 0:
                return False
            strategy_dayk = StrategyBasedOnDayKAction(dataset_240)
            valid_240_amplitude, result_amplitude_240 = strategy_dayk.executeaction(operation='amplitude_avg')
            if valid_240_amplitude:
                if len(result_amplitude_240) > 0:
                    append_value(self._data_context.totalresult[DataContext.strategy100], stock_symbol, CalcResult())
                else:
                    return False
            else:
                logger.error("strategy_amplitude_avg_240 is failed on {}".format(stock_symbol))
                return False
        return True


# MACD指标在60分钟周期上当前向上且快线大于慢线
class Strategy101(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result=None) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        macd_go_60 = StrategyBasedonMACDAction(dataset_60, 2)
        valid_60_macd_strict, result_macd_strict_60 = macd_go_60.executeaction(operation='strict')
        if valid_60_macd_strict:
            if len(result_macd_strict_60) > 0:
                return True
        else:
            logger.error("strategy_macd_strict_60 is failed on {}".format(stock_symbol))
    
        return False


# 20均线指标在60分钟周期上当前向上
# EXPMA指标在60分钟周期上快线当前向上
class Strategy102(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result=None) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        ma_go_60 = StrategyBasedOnDayKAction(dataset_60)
        valid_60_ma, result_ma_60 = ma_go_60.executeaction(operation='avg_k_go')
        if valid_60_ma:
            if len(result_ma_60) == 0:
                return False
        else:
            logger.error("strategy_ma_avg_60 is failed on {}".format(stock_symbol))
            return False
    
        expma_go_60 = StrategyBasedOnDayKAction(dataset_60)
        valid_60_expema_dif, result_expema_dif_60 = expma_go_60.executeaction(operation='expma_dif_go')
        if valid_60_expema_dif:
            if len(result_expema_dif_60) == 0:
                return False
        else:
            logger.error("strategy_expma_dif_go_60 is failed on {}".format(stock_symbol))
            return False
        
        return True


# MACD指标在60分钟周期上快线当前向上
class Strategy103(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result=None) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        macd_go_60 = StrategyBasedonMACDAction(dataset_60, 2)
        valid_60_macd_dif, result_macd_dif_60 = macd_go_60.executeaction(operation='dif')
        if valid_60_macd_dif:
            if len(result_macd_dif_60) > 0:
                return True
        else:
            logger.error("strategy_macd_diff_60 is failed on {}".format(stock_symbol))
        
        return False


# 1. 股价在60分钟周期上20均线指标之下
# 2. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠
class Strategy104(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
    
    def execute_action(self, stock_sector, stock_symbol, result=None) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        price_kavg_60 = StrategyBasedOnDayKAction(dataset_60)
        valid_60_price_ma, result_price_ma_60 = price_kavg_60.executeaction(operation='price_k_avg')
        if valid_60_price_ma:
            if len(result_price_ma_60) > 0:
                kd_60 = StrategyBasedOnKDAction(dataset_60)
                valid_60_entangle_period, result_entangle_period_60 = \
                    kd_60.executeaction(occurrence_time=[dataset_60.index[-1]],
                                        operation='entangle_within_period',
                                        periods=4,
                                        duration=40)
                if valid_60_entangle_period:
                    if len(result_entangle_period_60) > 0:
                        return True
                else:
                    logger.error("strategy_kd_entangle_60 is failed on {}".format(stock_symbol))
        else:
            logger.error("strategy_price_ma_60 is failed on {}".format(stock_symbol))
    
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. MACD指标在60分钟周期上当前向上且快线大于慢线
# 0. 20均线指标在60分钟周期上当前向上
# 0. EXPMA指标在60分钟周期上快线当前向上
# 1. 收盘价在15分钟周期上穿70均线
# 2. 成交量在15分钟周期大于80均线              (在merge函数中实现)
# 3. KD指标在30分钟周期形成金叉
class Strategy1(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_30 = self._data_context.data30mins[stock_sector].get(stock_symbol)
        dataset_15 = self._data_context.data15mins[stock_sector].get(stock_symbol)
        if len(dataset_30) == 0 or len(dataset_15) == 0:
            return False
        kd_cross_30 = StrategyBasedOnKDAction(dataset_30)
        kd_indicator_30 = KDAction(dataset_30, self._data_context.rsv_period, self._data_context.k_period,
                                   self._data_context.d_period)
        valid_kd_30, k_v_30, d_v_30 = kd_indicator_30.executeaction()
        ma_cross = CROSSUpMAAction(dataset_15)
        valid, result_tmp = ma_cross.executeaction(startindex=self._data_context.start_i,
                                                   endindex=self._data_context.end_i,
                                                   cross_period=self._data_context.cross_sma_period,
                                                   greater_period=self._data_context.greater_than_sma_period)
        if valid:
            if len(result_tmp) > 0:
                time_sequence = []
                for time_stamp_original in result_tmp['time'].array:
                    tmp_date = datetime.date(year=time_stamp_original.year, month=time_stamp_original.month,
                                             day=time_stamp_original.day)
                    if time_stamp_original.minute == 0:
                        time_stamp = time_stamp_original
                    elif time_stamp_original.minute <= 30:
                        time_stamp = pd.Timestamp(datetime.datetime.combine(tmp_date,
                                                                            datetime.time(
                                                                                hour=time_stamp_original.hour,
                                                                                minute=30)))
                    else:
                        time_stamp = pd.Timestamp(datetime.datetime.combine(tmp_date,
                                                                            datetime.time(
                                                                                hour=time_stamp_original.hour + 1)))
                    time_sequence.append(time_stamp)
            
                if not valid_kd_30:
                    logger.error("strategy_cross_kd_30 is failed on {}".format(stock_symbol))
                else:
                    valid, result_tmp = kd_cross_30.executeaction(occurrence_time=time_sequence,
                                                                  operation='cross_up',
                                                                  KValues=k_v_30,
                                                                  DValues=d_v_30,
                                                                  crossvalue=(False, 0))
                    if valid:
                        if len(result_tmp) > 0:
                            result[DataContext.strategy1] = result_tmp
                            return True
                    else:
                        logger.error("strategy_cross_kd_30 is failed on {}".format(stock_symbol))
        else:
            logger.error("strategy_cross_70 is failed on {}".format(stock_symbol))
        
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. MACD指标在60分钟周期上当前向上且快线大于慢线
# 0. 20均线指标在60分钟周期上当前向上
# 0. EXPMA指标在60分钟周期上快线当前向上
# 1. 收盘价在60分钟周期不大于50                 (在merge函数中实现)
# 2. KD指标在60分钟周期形成金叉且金叉小于30
class Strategy2(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        kd_60 = StrategyBasedOnKDAction(dataset_60)
        valid_60, result_tmp_60 = kd_60.executeaction(occurrence_time=[dataset_60.index[-1]],
                                                      operation='cross_up',
                                                      crossvalue=(True, 30))
        if valid_60:
            if len(result_tmp_60) > 0:
                result[DataContext.strategy2] = result_tmp_60
                return True
        else:
            logger.error("strategy_cross_kd_60 is failed on {}".format(stock_symbol))
        
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. MACD指标在60分钟周期上当前向上且快线大于慢线
# 0. 20均线指标在60分钟周期上当前向上
# 0. EXPMA指标在60分钟周期上快线当前向上
# 1. KD指标在60分钟周期至少持续纠缠4个周期且小于30
class Strategy3(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        kd_60 = StrategyBasedOnKDAction(dataset_60)
        valid_60_entangle, result_entangle_60 = kd_60.executeaction(occurrence_time=[dataset_60.index[-1]],
                                                                    operation='entangle',
                                                                    crossvalue=(True, 30),
                                                                    periods=4)
        if valid_60_entangle:
            if len(result_entangle_60) > 0:
                result[DataContext.strategy3] = result_entangle_60
                return True
        else:
            logger.error("strategy_entangle_kd_60 is failed on {}".format(stock_symbol))
        
        return False
    

# 0. 前五日价格振幅平均值大于等于3%
# 0. MACD指标在60分钟周期上当前向上且快线大于慢线
# 0. 20均线指标在60分钟周期上当前向上
# 0. EXPMA指标在60分钟周期上快线当前向上
# 1. KD指标在60分钟周期至少持续纠缠4个周期
# 2. KD指标在60分钟周期形成金叉
class Strategy4(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        kd_60 = StrategyBasedOnKDAction(dataset_60)
        valid_60_entangle_crossup, result_entangle_crossup_60 = \
            kd_60.executeaction(occurrence_time=[dataset_60.index[-1]],
                                operation='entangle_and_cross_up',
                                periods=4)
        if valid_60_entangle_crossup:
            if len(result_entangle_crossup_60) > 0:
                result[DataContext.strategy4] = result_entangle_crossup_60
                return True
        else:
            logger.error("strategy_entangle_crossup_kd_60 is failed on {}".format(stock_symbol))
        
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. MACD指标在60分钟周期上当前向上且快线大于慢线
# 0. 20均线指标在60分钟周期上当前向上
# 0. EXPMA指标在60分钟周期上快线当前向上
# 1. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠
# 2. KD指标在60分钟周期形成金叉
class Strategy5(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        kd_60 = StrategyBasedOnKDAction(dataset_60)
        valid_60_entangle_crossup_period, result_entangle_crossup_period_60 = \
            kd_60.executeaction(occurrence_time=[dataset_60.index[-1]],
                                operation='entangle_and_cross_up_within_period',
                                periods=4,
                                duration=40)
    
        if valid_60_entangle_crossup_period:
            if len(result_entangle_crossup_period_60) > 0:
                result[DataContext.strategy5] = result_entangle_crossup_period_60
                return True
        else:
            logger.error("strategy_entangle_crossup_period_kd_60 is failed on {}".format(stock_symbol))
        
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. MACD指标在60分钟周期上当前向上且快线大于慢线
# 0. 20均线指标在60分钟周期上当前向上
# 0. EXPMA指标在60分钟周期上快线当前向上
# 1. KD指标在30分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠
# 2. KD指标在30分钟周期形成金叉
class Strategy6(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_30 = self._data_context.data30mins[stock_sector].get(stock_symbol)
        if len(dataset_30) == 0:
            return False
        kd_cross_30 = StrategyBasedOnKDAction(dataset_30)
        kd_indicator_30 = KDAction(dataset_30,
                                   self._data_context.rsv_period,
                                   self._data_context.k_period,
                                   self._data_context.d_period)
        valid_kd_30, k_v_30, d_v_30 = kd_indicator_30.executeaction()
        if not valid_kd_30:
            valid_30_entangle_crossup_period, result_entangle_crossup_period_30 = \
                kd_cross_30.executeaction(occurrence_time=[dataset_30.index[-1]],
                                          operation='entangle_and_cross_up_within_period',
                                          KValues=k_v_30,
                                          DValues=d_v_30,
                                          periods=4,
                                          duration=80,
                                          crossvalue=(False, 0))
            if valid_30_entangle_crossup_period:
                if len(result_entangle_crossup_period_30) > 0:
                    result[DataContext.strategy6] = result_entangle_crossup_period_30
                    return True
            else:
                logger.error("strategy_entangle_crossup_kd_30 is failed on {}".format(stock_symbol))
        else:
            logger.error("strategy_entangle_crossup_kd_30 is failed on {}".format(stock_symbol))

        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. MACD指标在60分钟周期上快线当前向上
# 0. 20均线指标在60分钟周期上当前向上
# 0. EXPMA指标在60分钟周期上快线当前向上
# 1. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠
# 2. KD指标在60分钟周期形成金叉
class Strategy7(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        kd_60 = StrategyBasedOnKDAction(dataset_60)
        valid_60_entangle_crossup_period, result_entangle_crossup_period_60 = \
            kd_60.executeaction(occurrence_time=[dataset_60.index[-1]],
                                operation='entangle_and_cross_up_within_period',
                                periods=4,
                                duration=40)
    
        if valid_60_entangle_crossup_period:
            if len(result_entangle_crossup_period_60) > 0:
                result[DataContext.strategy7] = result_entangle_crossup_period_60
                return True
                
        else:
            logger.error("strategy_entangle_crossup_period_kd_60 is failed on {}".format(stock_symbol))
        
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 1. EXPMA指标在日线周期形成金叉
class Strategy8(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
    
    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_240 = self._data_context.data240mins[stock_sector].get(stock_symbol)
        if len(dataset_240) == 0:
            return False
        expma_cross_240 = EXPMACrossAction(dataset_240)
        valid_expma_240, value_240_expma = expma_cross_240.executeaction()
        if valid_expma_240:
            if len(value_240_expma) > 0:
                result[DataContext.strategy8] = value_240_expma
                return True
        else:
            logger.error("strategy_expma_cross_240 is failed on {}".format(stock_symbol))

        return False


# 0. 前五日价格振幅平均值大于等于3%
# 1. EXPMA指标在30分钟周期形成金叉
class Strategy9(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
        
    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_30 = self._data_context.data30mins[stock_sector].get(stock_symbol)
        if len(dataset_30) == 0:
            return False
        expma_cross_30 = EXPMACrossAction(dataset_30)
        valid_expma_30, value_30_expma = expma_cross_30.executeaction()
        if valid_expma_30:
            if len(value_30_expma) > 0:
                result[DataContext.strategy9] = value_30_expma
                return True
        else:
            logger.error("strategy_expma_cross_30 is failed on {}".format(stock_symbol))
        
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 1. EXPMA指标在60分钟周期形成金叉
class Strategy10(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
    
    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        expma_cross_60 = EXPMACrossAction(dataset_60)
        valid_expma_60, value_60_expma = expma_cross_60.executeaction()
        if valid_expma_60:
            if len(value_60_expma) > 0:
                result[DataContext.strategy10] = value_60_expma
                return True
        else:
            logger.error("strategy_expma_cross_60 is failed on {}".format(stock_symbol))
        
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 1. MACD指标在60分钟周期形成金叉
# 2. KD指标在60分钟周期形成金叉
class Strategy11(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
    
    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        kd_60 = StrategyBasedOnKDAction(dataset_60)
        valid_60_kd_cross, result_kd_cross_60 = kd_60. \
            executeaction(occurrence_time=[dataset_60.index[-1]],
                          operation='cross_up')
        if valid_60_kd_cross:
            if len(result_kd_cross_60) > 0:
                macd_cross_60 = StrategyBasedonMACDAction(dataset_60, 2)
                valid_60_macd_cross, result_macd_cross_60 = macd_cross_60.executeaction(operation='cross_up')
                if valid_60_macd_cross:
                    if len(result_macd_cross_60) > 0:
                        result[DataContext.strategy11] = result_macd_cross_60
                        return True
                else:
                    logger.error("strategy_macd_cross_up_60 is failed on {}".format(stock_symbol))
        else:
            logger.error("strategy_kd_cross_up_60 is failed on {}".format(stock_symbol))

        return False
    

# 0. 前五日价格振幅平均值大于等于3%
# 1. 当前K线值大于前五天的kd金叉的交叉点的5%
# 2. 当前收盘价比前五天内的最低价再低1%
class Strategy12(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
    
    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        kd_60 = StrategyBasedOnKDAction(dataset_60)
        valid_60_kd_deviate, result_kd_deviate_60 = kd_60. \
            executeaction(occurrence_time=[dataset_60.index[-1]],
                          operation='divergence_price_lower_and_k_higher_simple',
                          duration=20)
        if valid_60_kd_deviate:
            if len(result_kd_deviate_60) > 0:
                result[DataContext.strategy12] = result_kd_deviate_60
                return True
        else:
            logger.error("strategy_kd_deviate_60 is failed on {}".format(stock_symbol))

        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. 股价在60分钟周期上20均线指标之下
# 0. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠
# 1. KD指标在60分钟周期形成金叉
class Strategy13(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
    
    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_60) == 0:
            return False
        valid_60_crossup, result_crossup_60 = \
            StrategyBasedOnKDAction(dataset_60).executeaction(
                occurrence_time=[dataset_60.index[-1]], operation='cross_up')
        if valid_60_crossup:
            if len(result_crossup_60) > 0:
                result[DataContext.strategy13] = result_crossup_60
                return True
        else:
            logger.error("strategy_kd_cross_up_60 is failed on {}".format(stock_symbol))
    
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. 股价在60分钟周期上20均线指标之下
# 0. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠
# 1. KD指标在30分钟周期形成金叉
class Strategy14(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)
    
    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_30 = self._data_context.data30mins[stock_sector].get(stock_symbol)
        if len(dataset_30) == 0:
            return False
        valid_30_crossup, result_crossup_30 = \
            StrategyBasedOnKDAction(dataset_30).executeaction(
                occurrence_time=[dataset_30.index[-1]], operation='cross_up')
        if valid_30_crossup:
            if len(result_crossup_30) > 0:
                result[DataContext.strategy14] = result_crossup_30
                return True
        else:
            logger.error("strategy_kd_cross_up_30 is failed on {}".format(stock_symbol))

        return False


# 0. 成交量在15分钟周期大于5均线
# 0. KD指标在60分钟周期上在最近5天内至少存在一个连续4个周期的纠缠
class Strategy201(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_15 = self._data_context.data15mins[stock_sector].get(stock_symbol)
        dataset_60 = self._data_context.data60mins[stock_sector].get(stock_symbol)
        if len(dataset_15) == 0 or len(dataset_60) == 0:
            return False
        volume_15_values = dataset_15.volume.values
        avg_15_volume_5 = MA(volume_15_values, 5)
        if volume_15_values[-1] > avg_15_volume_5[-1]:
            kd_60 = StrategyBasedOnKDAction(dataset_60)
            valid_60_entangle_period, result_entangle_period_60 = \
                kd_60.executeaction(occurrence_time=[dataset_60.index[-1]],
                                    operation='entangle_within_period',
                                    periods=4,
                                    duration=20)
            if valid_60_entangle_period:
                if len(result_entangle_period_60) > 0:
                    return True
            else:
                logger.error("strategy_kd_entangle_60 is failed on {}".format(stock_symbol))
            
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. 成交量在15分钟周期大于5均线
# 0. KD指标在60分钟周期上在最近5天内至少存在一个连续4个周期的纠缠
# 1. KD指标在15分钟周期形成金叉且小于30
class Strategy20(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_15 = self._data_context.data15mins[stock_sector].get(stock_symbol)
        if len(dataset_15) == 0:
            return False
        valid_15_crossup, result_crossup_15 = \
            StrategyBasedOnKDAction(dataset_15).executeaction(occurrence_time=[dataset_15.index[-1]],
                                                              operation='cross_up',
                                                              crossvalue=(True, 30))
        if valid_15_crossup:
            if len(result_crossup_15) > 0:
                result[DataContext.strategy20] = result_crossup_15
                return True
        else:
            logger.error("strategy_kd_cross_up_15 is failed on {}".format(stock_symbol))
        
        return False


# 0. 前五日价格振幅平均值大于等于3%
# 0. 成交量在15分钟周期大于5均线
# 0. KD指标在60分钟周期上在最近5天内至少存在一个连续4个周期的纠缠
# 1. KD指标在15分钟周期形成第一个金叉后又在60个15分钟周期内再次形成金叉且小于30
class Strategy21(CompositeStrategyBase):
    def __init__(self, data_context: DataContext):
        super().__init__(data_context)

    def execute_action(self, stock_sector, stock_symbol, result) -> bool:
        dataset_15 = self._data_context.data15mins[stock_sector].get(stock_symbol)
        if len(dataset_15) == 0:
            return False
        valid_15_crossup, result_crossup_15 = \
            StrategyBasedOnKDAction(dataset_15).executeaction(occurrence_time=[dataset_15.index[-1]],
                                                              operation='cross_up_twice',
                                                              cross_twice_period=60,
                                                              crossvalue=(True, 30))
        if valid_15_crossup:
            if len(result_crossup_15) > 0:
                result[DataContext.strategy21] = result_crossup_15
                return True
        else:
            logger.error("strategy_kd_cross_up_15 is failed on {}".format(stock_symbol))

        return False


class StrategyTree:
    def __init__(self, strategy: CompositeStrategyBase):
        self.strategy = strategy
        self.children: list = []


class StrategyContext:
    strategy_tree: StrategyTree = None

    @classmethod
    def initklz(cls, data_context: DataContext):
        # s100
        StrategyContext.strategy_tree = StrategyTree(Strategy100(data_context))
        
        # s101
        intermediate_node_101 = StrategyTree(Strategy101(data_context))
        intermediate_node_101.children = [
                                          # StrategyTree(Strategy1(data_context)),
                                          # StrategyTree(Strategy2(data_context)),
                                          # StrategyTree(Strategy3(data_context)),
                                          StrategyTree(Strategy4(data_context)),
                                          StrategyTree(Strategy5(data_context))
                                          # StrategyTree(Strategy6(data_context))
                                         ]
        
        # s103
        intermediate_node_103 = StrategyTree(Strategy103(data_context))
        intermediate_node_103.children = [StrategyTree(Strategy7(data_context))]

        # s102
        intermediate_node_102 = StrategyTree(Strategy102(data_context))
        intermediate_node_102.children = [intermediate_node_101, intermediate_node_103]
        
        # s104
        intermediate_node_104 = StrategyTree(Strategy104(data_context))
        intermediate_node_104.children = [StrategyTree(Strategy13(data_context)),
                                          StrategyTree(Strategy14(data_context))]
        
        # s201
        intermediate_node_201 = StrategyTree(Strategy201(data_context))
        intermediate_node_201.children = [StrategyTree(Strategy20(data_context)),
                                          StrategyTree(Strategy21(data_context))]
        
        StrategyContext.strategy_tree.children = [intermediate_node_201,
                                                  intermediate_node_102,
                                                  intermediate_node_104,
                                                  StrategyTree(Strategy8(data_context)),
                                                  StrategyTree(Strategy9(data_context)),
                                                  StrategyTree(Strategy10(data_context)),
                                                  StrategyTree(Strategy11(data_context)),
                                                  StrategyTree(Strategy12(data_context))]

@time_measure
def quantstrategies(context: DataContext):
    totalresultdata = {}
    
    for sector_usd in context.markets:
        resultdata = {}
        sector_tmp = stock_group[sector_usd]
        for symbol_tmp in context.symbols[sector_tmp]:
            try:
                result = {}
                runStrategies(StrategyContext.strategy_tree, symbol_tmp, sector_tmp, result)
                resultdata[symbol_tmp] = result
            except BaseException as be:
                logger.debug("runStrategies is failed, symbol is {}".format(symbol_tmp))
                logger.error("runStrategies is failed, symbol is {}".format(symbol_tmp), be)
        totalresultdata[sector_tmp] = resultdata
        
    DataContext.is_first_time_to_run = False
    
    return totalresultdata


def runStrategies(strategies: StrategyTree, symbol_tmp, sector_tmp, result):
    if strategies is None or strategies.strategy is None:
        return
    if strategies.strategy.execute_action(sector_tmp, symbol_tmp, result):
        for child in strategies.children:
            runStrategies(child, symbol_tmp, sector_tmp, result)


# FIXME because EM has been obsoleted.
def calcrankofchange():
    if DataContext.iscountryChina():
        prefix = "B_"
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        # 偏移N天交易日
        date_offset = c.getdate(current_date, -11, "Market=CNSESH")
        if date_offset.ErrorCode != 0:
            logger.error("ErrorCode is %d and ErrorMsg is %s" % (date_offset.ErrorCode, date_offset.ErrorMsg))
            return False
        # 区间涨跌幅(流通市值加权平均):CPPCTCHANGEFMWAVG 区间资金净流入:PNETINFLOWSUM
        sectors_q = list(sectors_CN.keys())
        i = 1
        sectors_length = len(sectors_q) - 6
        sectors_v = []
        while i < sectors_length:
            j = i + 6
            if j > sectors_length:
                j = sectors_length
            sectors_g = ",".join(map(lambda x: prefix + x, sectors_q[i:j]))
            sector_data = c.cses(sectors_g, "CPPCTCHANGEFMWAVG,PNETINFLOWSUM",
                                 "StartDate={},EndDate={}, IsHistory=0, Ispandas=1, ShowBlank=0".format(
                                     date_offset.Data[0], current_date))
            sectors_v.append(sector_data)
            i += 6
        logger.debug("%d sectors has been scanned" % (sectors_length - 1))
        sectors_df = pd.concat(sectors_v)
        sectors_df_change_d = sectors_df.sort_values(by='CPPCTCHANGEFMWAVG', ascending=False)
        sectors_df_mf_d = sectors_df.sort_values(by='PNETINFLOWSUM', ascending=False)
        sectors_list_change_d = sectors_df_change_d.index.tolist()
        sectors_list_mf_d = sectors_df_mf_d.index.tolist()
        if len(sectors_df) > 50:
            list_sectors_change = sectors_list_change_d[:50]
            list_sectors_change_r = sectors_list_change_d[:-51:-1]
            list_sectors_mf = sectors_list_mf_d[:50]
            list_sectors_mf_r = sectors_list_mf_d[:-51:-1]
        else:
            list_sectors_change = sectors_list_change_d
            list_sectors_change_r = sectors_list_change_d[::-1]
            list_sectors_mf = sectors_list_mf_d
            list_sectors_mf_r = sectors_list_mf_d[::-1]

        e_subject = "版块排名_" + datetime.datetime.now().strftime("%Y%m%d")
        e_content = ""
        filepath = os.path.join(DataContext.dir_name, e_subject)
        with open(filepath, 'w+') as file:
            tmp_str = "涨幅版块排名\r\n"
            file.write(tmp_str)
            e_content += tmp_str
            for index in list_sectors_change:
                column = sectors_df_change_d['CPPCTCHANGEFMWAVG']
                sector_name = sectors_CN[index.lstrip(prefix)]
                tmp_str = "版块名称: {} -- 幅度: {}% \r\n".format(sector_name, column[index])
                file.write(tmp_str)
                e_content += tmp_str
            tmp_str = "\r\n跌幅版块排名\r\n"
            file.write(tmp_str)
            e_content += tmp_str
            for index in list_sectors_change_r:
                column = sectors_df_change_d['CPPCTCHANGEFMWAVG']
                sector_name = sectors_CN[index.lstrip(prefix)]
                tmp_str = "版块名称: {} -- 幅度: {}% \r\n".format(sector_name, column[index])
                file.write(tmp_str)
                e_content += tmp_str
            tmp_str = "\r\n资金净流入版块排名 - 从高到低\r\n"
            file.write(tmp_str)
            e_content += tmp_str
            for index in list_sectors_mf:
                column = sectors_df_mf_d['PNETINFLOWSUM']
                sector_name = sectors_CN[index.lstrip(prefix)]
                tmp_str = "版块名称: {} -- 资金: {} \r\n".format(sector_name, column[index])
                file.write(tmp_str)
                e_content += tmp_str
            tmp_str = "\r\n资金净流入版块排名 - 从低到高\r\n"
            file.write(tmp_str)
            e_content += tmp_str
            for index in list_sectors_mf_r:
                column = sectors_df_mf_d['PNETINFLOWSUM']
                sector_name = sectors_CN[index.lstrip(prefix)]
                tmp_str = "版块名称: {} -- 资金: {} \r\n".format(sector_name, column[index])
                file.write(tmp_str)
                e_content += tmp_str

        sendemail(e_subject, e_content, DataContext.email_recipient)
        sendemail(e_subject, e_content, DataContext.email_other1_recipient)
        sendemail(e_subject, e_content, DataContext.email_other2_recipient)


def summarytotalresult(context: DataContext):
    e_subject = "预警汇总_" + datetime.datetime.now().strftime("%Y%m%d")
    e_content = ""
    filepath = os.path.join(DataContext.dir_name, e_subject)
    with open(filepath, 'w+') as file:
        for strategy_t, symbols in context.totalresult.items():
            str101 = ""
            if strategy_t == DataContext.strategy6:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略6 - 30分钟周期:\r\n"
            elif strategy_t == DataContext.strategy20:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略20:\r\n"
            elif strategy_t == DataContext.strategy21:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略21:\r\n"
            elif strategy_t == DataContext.strategy14:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略14 - 30分钟周期:\r\n"
            elif strategy_t == DataContext.strategy13:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略13 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy12:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略12 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy11:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略11 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy10:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略10 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy9:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略9 - 30分钟周期:\r\n"
            elif strategy_t == DataContext.strategy8:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略8 - 日周期:\r\n"
            elif strategy_t == DataContext.strategy7:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略7 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy5:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略5 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy4:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略4 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy3:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略3 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy1:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略1 - 15分钟周期:\r\n"
            elif strategy_t == DataContext.strategy2:
                str101 = "\r\n\r\n\r\n\r\n\r\n策略2 - 60分钟周期:\r\n"
            elif strategy_t == DataContext.strategy1_2:
                str101 = "\r\n\r\n\r\n\r\n\r\n同时满足策略1和策略2的预警条件:\r\n\r\n"
            elif strategy_t == DataContext.strategy1_4:
                str101 = "\r\n\r\n\r\n\r\n\r\n同时满足策略1和策略4的预警条件:\r\n\r\n"
            if str101 != "":
                file.write(str101)
                e_content += str101
                symbols_str = " ".join(symbols.keys())
                file.write(symbols_str)
                e_content += symbols_str
    sendemail(e_subject, e_content, DataContext.email_recipient)
    # FIXME
    # sendemail(e_subject, e_content, DataContext.email_other1_recipient)
    # sendemail(e_subject, e_content, DataContext.email_other2_recipient)


# the function runs in a separate thread
@time_measure
def handleresult(context: DataContext):
    while True:
        resultfromq = context.queue.get()
        if isinstance(resultfromq, ProcessStatus) and resultfromq == ProcessStatus.STOP:
            logger.debug("The thread of handleresult quits")
            break
        subject_e1, content_e1, subject_e2, content_e2 = handleresultlocked(mergeresult(context, resultfromq), context)

        logger.debug("handleresultlocked was done")
        # send it via sina email
        time_cur = datetime.datetime.now()
        if datetime.datetime.combine(datetime.date(year=time_cur.year, month=time_cur.month, day=time_cur.day),
                                     context.marketclosetime) - time_cur <= datetime.timedelta(minutes=DataContext.sendemial_interval) \
                 or context.sendemailtime is None \
                 or time_cur - context.sendemailtime >= datetime.timedelta(minutes=DataContext.sendemial_interval):
            sendemail(subject_e1, content_e1, DataContext.email_recipient)
            context.sendemailtime = time_cur


class CalcResult:
    def __init__(self, ctime=None, isvgreater=False):
        self.cross_time = ctime
        self.isgreater_v = isvgreater


def mergeresult(context: DataContext, result_transient, ishistory: bool = False):
    def assembleFunc(symbol, strategy: str):
        symbol_s = str(symbol)
        symbols[strategy].append(symbol_s)
        if ishistory:
            append_value(context.totalresult[strategy], symbol_s, CalcResult(row[6], row[7]))
        else:
            append_value(context.totalresult[strategy], symbol_s, CalcResult(keytime, row[7]))

    def calcresult(strategy_n: str):
        # result_c = set(symbols[strategy_n]).intersection(result_c_s100)
        result_c = set(symbols[strategy_n])
        logger.info("%d symbols found with %s at %s" % (len(result_c), strategy_n, keytime))
        return result_c

    # result_h_s1 = set(context.totalresult[DataContext.strategy1].keys())
    # result_h_s2 = set(context.totalresult[DataContext.strategy2].keys())
    # result_h_s3 = set(context.totalresult[DataContext.strategy3].keys())
    result_h_s4 = set(context.totalresult[DataContext.strategy4].keys())
    result_h_s5 = set(context.totalresult[DataContext.strategy5].keys())
    result_h_s7 = set(context.totalresult[DataContext.strategy7].keys())
    result_h_s8 = set(context.totalresult[DataContext.strategy8].keys())
    result_h_s9 = set(context.totalresult[DataContext.strategy9].keys())
    result_h_s10 = set(context.totalresult[DataContext.strategy10].keys())
    result_h_s11 = set(context.totalresult[DataContext.strategy11].keys())
    result_h_s12 = set(context.totalresult[DataContext.strategy12].keys())
    result_h_s13 = set(context.totalresult[DataContext.strategy13].keys())
    result_h_s14 = set(context.totalresult[DataContext.strategy14].keys())
    result_h_s20 = set(context.totalresult[DataContext.strategy20].keys())
    result_h_s21 = set(context.totalresult[DataContext.strategy21].keys())

    keytime = datetime.datetime.now()
    symbols = {DataContext.strategy1: [], DataContext.strategy2: [], DataContext.strategy3: [],
               DataContext.strategy4: [], DataContext.strategy5: [], DataContext.strategy6: [],
               DataContext.strategy7: [], DataContext.strategy8: [], DataContext.strategy9: [],
               DataContext.strategy10: [], DataContext.strategy11: [], DataContext.strategy12: [],
               DataContext.strategy13: [], DataContext.strategy14: [], DataContext.strategy20: [],
               DataContext.strategy21: []}

    for time_result, result in result_transient.items():
        keytime = time_result
        for index, value in result.items():
            for index_1, value_1 in value.items():
                for index_2, value_2 in value_1.items():
                    '''
                    if index_2 == DataContext.strategy1:
                        for row in value_2.itertuples(index=False):
                            if row[7]:
                                assembleFunc(index_1, DataContext.strategy1)
                    elif index_2 == DataContext.strategy2:
                        for row in value_2.itertuples(index=False):
                            if row[2] <= 50:
                                assembleFunc(index_1, DataContext.strategy2)
                    elif index_2 == DataContext.strategy3:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy3)
                    '''
                    if index_2 == DataContext.strategy4:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy4)
                    elif index_2 == DataContext.strategy5:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy5)
                    elif index_2 == DataContext.strategy7:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy7)
                    elif index_2 == DataContext.strategy8:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy8)
                    elif index_2 == DataContext.strategy9:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy9)
                    elif index_2 == DataContext.strategy10:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy10)
                    elif index_2 == DataContext.strategy11:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy11)
                    elif index_2 == DataContext.strategy12:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy12)
                    elif index_2 == DataContext.strategy13:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy13)
                    elif index_2 == DataContext.strategy14:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy14)
                    elif index_2 == DataContext.strategy20:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy20)
                    elif index_2 == DataContext.strategy21:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy21)
                    '''
                    elif index_2 == DataContext.strategy6:
                        for row in value_2.itertuples(index=False):
                            assembleFunc(index_1, DataContext.strategy6)
                    '''
                    
    # result_c_s1 = calcresult(DataContext.strategy1)
    # result_c_s2 = calcresult(DataContext.strategy2)
    # result_c_s3 = calcresult(DataContext.strategy3)
    result_c_s4 = calcresult(DataContext.strategy4)
    result_c_s5 = calcresult(DataContext.strategy5)
    result_c_s7 = calcresult(DataContext.strategy7)
    result_c_s8 = calcresult(DataContext.strategy8)
    result_c_s9 = calcresult(DataContext.strategy9)
    result_c_s10 = calcresult(DataContext.strategy10)
    result_c_s11 = calcresult(DataContext.strategy11)
    result_c_s12 = calcresult(DataContext.strategy12)
    result_c_s13 = calcresult(DataContext.strategy13)
    result_c_s14 = calcresult(DataContext.strategy14)
    result_c_s20 = calcresult(DataContext.strategy20)
    result_c_s21 = calcresult(DataContext.strategy21)
    # result_c_s6 = calcresult(DataContext.strategy6)
    # result_c_s1_2 = result_c_s1.intersection(result_c_s2).union(result_c_s1.intersection(result_h_s2))
    # result_h_s1_2 = result_h_s1.intersection(result_h_s2).union(result_h_s1.intersection(result_c_s2))
    # for result_1_2 in result_c_s1_2:
    #     append_value(context.totalresult[DataContext.strategy1_2], result_1_2, CalcResult(keytime, True))
    # logger.info("%d symbols found with strategy 1 and 2 at %s" % (len(result_c_s1_2), keytime))
    # result_c_s1_4 = result_c_s1.intersection(result_c_s4).union(result_c_s1.intersection(result_h_s4))
    # result_h_s1_4 = result_h_s1.intersection(result_h_s4).union(result_h_s1.intersection(result_c_s4))
    # for result_1_4 in result_c_s1_4:
    #     append_value(context.totalresult[DataContext.strategy1_4], result_1_4, CalcResult(keytime, True))
    # logger.info("%d symbols found with strategy 1 and 4 at %s" % (len(result_c_s1_4), keytime))
    '''
    ret = {keytime: {DataContext.strategy5: [result_c_s5, result_h_s5],
                     DataContext.strategy4: [result_c_s4, result_h_s4],
                     DataContext.strategy1_4: [result_c_s1_4, result_h_s1_4],
                     DataContext.strategy3: [result_c_s3, result_h_s3],
                     DataContext.strategy1_2: [result_c_s1_2, result_h_s1_2],
                     DataContext.strategy1: [result_c_s1, result_h_s1],
                     DataContext.strategy2: [result_c_s2, result_h_s2],
                     # DataContext.strategy6: [result_c_s6, result_h_s6]}}
    '''
    ret = {keytime: {DataContext.strategy20: [result_c_s20, result_h_s20],
                     DataContext.strategy21: [result_c_s21, result_h_s21],
                     DataContext.strategy14: [result_c_s14, result_h_s14],
                     DataContext.strategy13: [result_c_s13, result_h_s13],
                     DataContext.strategy12: [result_c_s12, result_h_s12],
                     DataContext.strategy11: [result_c_s11, result_h_s11],
                     DataContext.strategy8: [result_c_s8, result_h_s8],
                     DataContext.strategy10: [result_c_s10, result_h_s10],
                     DataContext.strategy9: [result_c_s9, result_h_s9],
                     DataContext.strategy7: [result_c_s7, result_h_s7],
                     DataContext.strategy5: [result_c_s5, result_h_s5],
                     DataContext.strategy4: [result_c_s4, result_h_s4]}}
    return ret


def handleresultlocked(resultf, context: DataContext):
    emailcontent = ""
    emailcontent_em = ""

    def sortout(result_t: list):
        max_num = 5
        result_ch = {}
        email_c: str = ""
        email_p: str = ""
        none_sector = "无归属版块"
        if DataContext.iscountryChina():
            sectornames = sectors_CN
            spotlightedsector = sectornames['000001']
            print_order = [spotlightedsector]
        else:
            sectornames = sectors_US
            spotlightedsector = sectornames['000001']
            print_order = [spotlightedsector]
        for symbol_c in result_t:
            for index_s, value_s in context.sectors.items():
                if (isinstance(value_s, list) and symbol_c in value_s) or symbol_c == value_s:
                    append_value(result_ch, sectornames[index_s], symbol_c)
                    break
            else:
                append_value(result_ch, none_sector, symbol_c)
        for index_s in result_ch:
            if index_s != none_sector and index_s != spotlightedsector:
                print_order.append(index_s)
        else:
            print_order.append(none_sector)
        for index_s_o in print_order:
            if index_s_o not in result_ch:
                continue
            value_s_o = result_ch[index_s_o]
            str2 = "%s:\r\n" % index_s_o
            email_c += str2
            file.write(str2)
            if isinstance(value_s_o, list):
                while len(value_s_o) > 0:
                    if len(value_s_o) > max_num:
                        list_tmp = []
                        for i in range(max_num):
                            list_tmp.append(value_s_o.pop())
                        str3 = " ".join(list_tmp)
                        str_p = " ".join(list_tmp)
                    else:
                        str3 = " ".join(value_s_o)
                        str_p = " ".join(value_s_o)
                        value_s_o.clear()
                    str3 += "\r\n"
                    str_p += "\r\n"
                    email_c += str3
                    email_p += str_p
                    file.write(str3)
            else:
                str3 = value_s_o + "\r\n"
                str_p = value_s_o + "\r\n"
                email_c += str3
                email_p += str_p
                file.write(str3)
        return email_c, email_p

    def output(criteria):
        emailcontent_i = ""
        emailcontent_em_i = ""
        emailcontent_i += criteria
        file.write(criteria)
        str7 = "当前满足条件的股票代码:\r\n\r\n"
        emailcontent_i += str7
        file.write(str7)
        result_hm, result_em = sortout(symbols_l[0])
        emailcontent_i += result_hm
        emailcontent_em_i += result_em
        str8 = "\r\n\r\n"
        emailcontent_i += str8
        file.write(str8)
        str9 = "今日历史上满足条件的股票代码:\r\n\r\n"
        emailcontent_i += str9
        file.write(str9)
        result_hm, result_em = sortout(symbols_l[1])
        emailcontent_i += result_hm
        return emailcontent_i, emailcontent_em_i

    # output format symbol
    for time_result, result in resultf.items():
        filename = "result_" + time_result.strftime("%Y%m%d_%H%M")
        filename_em = "EM_" + filename
        filepath = os.path.join(DataContext.dir_name, filename)
        with open(filepath, 'w+') as file:
            for strategy_t, symbols_l in result.items():
                str101 = ""
                if strategy_t == DataContext.strategy6:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略6 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. MACD指标在60分钟周期上当前向上且快线大于慢线\r\n"
                    str101 += "  0. 20均线指标在60分钟周期上当前向上\r\n"
                    str101 += "  0. EXPMA指标在60分钟周期上快线当前向上\r\n"
                    str101 += "  1. KD指标在30分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠\r\n"
                    str101 += "  2. KD指标在30分钟周期形成金叉\r\n\r\n"
                elif strategy_t == DataContext.strategy20:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略20 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. 成交量在15分钟周期大于5均线\r\n"
                    str101 += "  0. KD指标在60分钟周期上在最近5天内至少存在一个至少连续4个周期的纠缠\r\n"
                    str101 += "  1. KD指标在15分钟周期形成金叉且小于30\r\n\r\n"
                elif strategy_t == DataContext.strategy21:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略21 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. 成交量在15分钟周期大于5均线\r\n"
                    str101 += "  0. KD指标在60分钟周期上在最近5天内至少存在一个至少连续4个周期的纠缠\r\n"
                    str101 += "  1. KD指标在15分钟周期形成第一个金叉后又在60个15分钟周期内再次形成金叉且小于30\r\n\r\n"
                elif strategy_t == DataContext.strategy14:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略14 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  1. 股价在60分钟周期上20均线指标之下\r\n"
                    str101 += "  2. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠\r\n"
                    str101 += "  3. KD指标在30分钟周期形成金叉\r\n\r\n"
                elif strategy_t == DataContext.strategy13:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略13 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  1. 股价在60分钟周期上20均线指标之下\r\n"
                    str101 += "  2. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠\r\n"
                    str101 += "  3. KD指标在60分钟周期形成金叉\r\n\r\n"
                elif strategy_t == DataContext.strategy12:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略12 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  1. 当前K线值大于前五天的kd金叉的交叉点的5%\r\n"
                    str101 += "  2. 当前收盘价比前五天内的最低价再低1%\r\n"
                elif strategy_t == DataContext.strategy11:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略11 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  1. MACD指标在60分钟周期形成金叉\r\n"
                    str101 += "  2. KD指标在60分钟周期形成金叉\r\n"
                elif strategy_t == DataContext.strategy10:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略10 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  1. EXPMA指标在60分钟周期形成金叉\r\n"
                elif strategy_t == DataContext.strategy9:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略9 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  1. EXPMA指标在30分钟周期形成金叉\r\n"
                elif strategy_t == DataContext.strategy8:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略8 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  1. EXPMA指标在日线周期形成金叉\r\n"
                elif strategy_t == DataContext.strategy7:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略7 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. MACD指标在60分钟周期上快线当前向上\r\n"
                    str101 += "  0. 20均线指标在60分钟周期上当前向上\r\n"
                    str101 += "  0. EXPMA指标在60分钟周期上快线当前向上\r\n"
                    str101 += "  1. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠\r\n"
                    str101 += "  2. KD指标在60分钟周期形成金叉\r\n\r\n"
                elif strategy_t == DataContext.strategy5:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略5 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. MACD指标在60分钟周期上当前向上且快线大于慢线\r\n"
                    str101 += "  0. 20均线指标在60分钟周期上当前向上\r\n"
                    str101 += "  0. EXPMA指标在60分钟周期上快线当前向上\r\n"
                    str101 += "  1. KD指标在60分钟周期上在最近10天内至少存在一个至少连续4个周期的纠缠\r\n"
                    str101 += "  2. KD指标在60分钟周期形成金叉\r\n\r\n"
                elif strategy_t == DataContext.strategy4:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略4 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. MACD指标在60分钟周期上当前向上且快线大于慢线\r\n"
                    str101 += "  0. 20均线指标在60分钟周期上当前向上\r\n"
                    str101 += "  0. EXPMA指标在60分钟周期上快线当前向上\r\n"
                    str101 += "  1. KD指标在60分钟周期至少持续纠缠4个周期\r\n"
                    str101 += "  2. KD指标在60分钟周期形成金叉\r\n\r\n"
                elif strategy_t == DataContext.strategy3:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略3 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. MACD指标在60分钟周期上当前向上且快线大于慢线\r\n"
                    str101 += "  0. 20均线指标在60分钟周期上当前向上\r\n"
                    str101 += "  0. EXPMA指标在60分钟周期上快线当前向上\r\n"
                    str101 += "  1. KD指标在60分钟周期至少持续纠缠4个周期且小于30\r\n\r\n"
                elif strategy_t == DataContext.strategy1:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略1 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. MACD指标在60分钟周期上当前向上且快线大于慢线\r\n"
                    str101 += "  0. 20均线指标在60分钟周期上当前向上\r\n"
                    str101 += "  0. EXPMA指标在60分钟周期上快线当前向上\r\n"
                    str101 += "  1. 收盘价在15分钟周期上穿70均线\r\n"
                    str101 += "  2. 成交量在15分钟周期大于80均线\r\n"
                    str101 += "  3. KD指标在30分钟周期形成金叉\r\n\r\n"
                    # str101 += "  4. OBV指标在30分钟周期大于零且大于30天均值\r\n\r\n"
                elif strategy_t == DataContext.strategy2:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略2 - 预警条件为:\r\n"
                    str101 += "  0. 前五日价格振幅平均值大于等于3%\r\n"
                    str101 += "  0. MACD指标在60分钟周期上当前向上且快线大于慢线\r\n"
                    str101 += "  0. 20均线指标在60分钟周期上当前向上\r\n"
                    str101 += "  0. EXPMA指标在60分钟周期上快线当前向上\r\n"
                    str101 += "  1. 收盘价在60分钟周期不大于50\r\n"
                    str101 += "  2. KD指标在60分钟周期形成金叉且金叉小于30\r\n\r\n"
                elif strategy_t == DataContext.strategy1_2:
                    str101 = "\r\n\r\n\r\n\r\n\r\n同时满足策略1和策略2的预警条件:\r\n\r\n"
                elif strategy_t == DataContext.strategy1_4:
                    str101 = "\r\n\r\n\r\n\r\n\r\n同时满足策略1和策略4的预警条件:\r\n\r\n"
                content_tmp, content_em_tmp = output(str101)
                emailcontent += content_tmp
                emailcontent_em += content_em_tmp

    return filename, emailcontent, filename_em, emailcontent_em


def sendemail(email_subject: str, email_content: str, recipient: str):
    msg = EmailMessage()
    msg["From"] = DataContext.email_sender
    msg["To"] = recipient
    msg["Subject"] = email_subject
    msg.set_content(email_content)
    try:
        with smtplib.SMTP_SSL(DataContext.smtp_address,
                              DataContext.smtp_port) as smtp:
            smtp.set_debuglevel(1)
            smtp.login(DataContext.email_sender, DataContext.email_pwd)
            smtp.send_message(msg)
    except Exception as ee:
        logger.error("error >>>", ee)
        traceback.print_exc()
    else:
        logger.info("Send %s an email %s successfully." % (recipient, email_subject))


def calconhistory(context: DataContext):
    login_em()
    loadsectors(context)
    result = {datetime.datetime.now(): quantstrategies(context)}
    handleresultlocked(mergeresult(context, result, ishistory=True), context)
    logout_em()


def updatedatabaselocked(board: list, source: DataSource = DataSource.AK_SHARE):
    retriedstocks = {}
    loaddatainternal(board, 2, retried=retriedstocks, datasource=source)
    reloaddata(retriedstocks)
    logger.debug("Downloading data is done and thread id is {} and thread ident is {}".
                 format(threading.get_native_id(), threading.get_ident()))


def updatedatabase(is_auto=False, source: DataSource = DataSource.AK_SHARE, context: DataContext=None):
    if is_auto:
        timedelta = datetime.timedelta(minutes=10)
        today = datetime.date.today()
        time_download = datetime.datetime.combine(today, datetime.time(hour=18))
        while True:
            if (datetime.datetime.now() - time_download) > timedelta:
                break
            else:
                time.sleep(600)

    if DataContext.iscountryChina():
        if is_auto and source == DataSource.AK_SHARE:
            loaddatainternal(DataContext.markets, 5, 5,
                             datasource=source, context=context)
            return

        if source == DataSource.EFINANCE:
            updatedatabaselocked(DataContext.markets, source)
        else:
            global queue_history_data
            reloaded_symbols = []

            def execute_data_task():
                queue_history_data.put((ProcessStatus.START, threading.get_ident()))
                updatedatabaselocked(DataContext.markets, source)
                queue_history_data.put((ProcessStatus.STOP, threading.get_ident()))

            def execute_continue_data_task():
                pass

            def execute_watchdog_task():
                last_board_symbol: tuple
                timer: threading.Timer = None

                def timer_func(thread_id: int):
                    if last_board_symbol is not None:
                        reloaded_symbols.append(last_board_symbol)
                        logger.debug("%s - %s needs to be reloaded." % last_board_symbol)

                while True:
                    resultfromq = queue_history_data.get()
                    if isinstance(resultfromq, tuple) and len(resultfromq) == 2:
                        result_1 = resultfromq[0]
                        result_2 = resultfromq[1]
                        if isinstance(result_1, ProcessStatus) and result_1 == ProcessStatus.STOP \
                                and isinstance(result_2, int):
                            logger.debug("The thread {} of fetching data quited".format(result_2))
                            # stop timer
                            if timer is not None:
                                timer.cancel()
                            break
                        elif isinstance(result_1, ProcessStatus) and resultfromq[0] == ProcessStatus.START \
                                and isinstance(result_2, int):
                            logger.debug("The thread {} of fetching data was started".format(result_2))
                            # start timer
                            timer = threading.Timer(300, timer_func, (result_2,))
                            timer.start()
                        elif isinstance(result_1, tuple) and len(result_1) == 2 and isinstance(result_2,
                                                                                               threading.Event):
                            last_board_symbol = result_1
                            logger.debug("%s - %s has been downloaded." % last_board_symbol)
                            # restart timer
                            if timer is not None:
                                timer.cancel()
                            timer = threading.Timer(5, timer_func, (result_2,))
                            timer.start()
                            result_2.set()

            def start_thread(target_func: list):
                for tf in target_func:
                    thread = threading.Thread(target=tf)
                    thread.start()
                    threads.append(thread)

            threads = []

            '''
            for market in DataContext.markets:
                thread = threading.Thread(target=updatedatabaselocked, args=(market, source,))
                thread.start()
                threads.append(thread)
            '''
            # East Money was obsoleted.
            # loaddata(DataContext.markets, 2, datasource=DataSource.EAST_MONEY, period=240, type_func=2, isloginAlready=True)

            start_thread([execute_watchdog_task, execute_data_task])
            for i in range(len(threads)):
                threads[i].join()
        if not is_auto and getdbconn():
            getdbconn().cursor().close()
            getdbconn().close()
            logger.debug("PostgreSQL connection is closed")

    if DataContext.iscountryUS():
        loaddata(DataContext.markets, 2, datasource=DataSource.YAHOO)


def login_em(isforcelogin: bool=True):
    def mainCallback(quantdata):
        """
        mainCallback 是主回调函数，可捕捉如下错误
        在start函数第三个参数位传入，该函数只有一个为c.EmQuantData类型的参数quantdata
        :param quantdata:c.EmQuantData
        :return:
        """
        logger.debug("mainCallback", str(quantdata))
        # 登录掉线或者 登陆数达到上线（即登录被踢下线） 这时所有的服务都会停止
        if str(quantdata.ErrorCode) == "10001011" or str(quantdata.ErrorCode) == "10001009":
            logger.error("Your account is disconnect. You can force login automatically here if you need.")
        # 行情登录验证失败（每次连接行情服务器时需要登录验证）或者行情流量验证失败时，会取消所有订阅，用户需根据具体情况处理
        elif str(quantdata.ErrorCode) == "10001021" or str(quantdata.ErrorCode) == "10001022":
            logger.error("Your all csq subscribe have stopped.")
        # 行情服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
        elif str(quantdata.ErrorCode) == "10002009":
            logger.error("Your all csq subscribe have stopped, reconnect 6 times fail.")
        # 行情订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_QUOTE_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
        elif str(quantdata.ErrorCode) == "10002012":
            logger.error("csq subscribe break on some error, reconnect and request automatically.")
        # 资讯服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
        elif str(quantdata.ErrorCode) == "10002014":
            logger.error("Your all cnq subscribe have stopped, reconnect 6 times fail.")
        # 资讯订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_INFO_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
        elif str(quantdata.ErrorCode) == "10002013":
            logger.error("cnq subscribe break on some error, reconnect and request automatically.")
        # 资讯登录验证失败（每次连接资讯服务器时需要登录验证）或者资讯流量验证失败时，会取消所有订阅，用户需根据具体情况处理
        elif str(quantdata.ErrorCode) == "10001024" or str(quantdata.ErrorCode) == "10001025":
            logger.error("Your all cnq subscribe have stopped.")
        else:
            pass

    try:
        # 调用登录函数（激活后使用，不需要用户名密码）
        if isforcelogin:
            loginparam = "TestLatency=1,ForceLogin=1"
        else:
            loginparam = "TestLatency=1"
        loginResult = c.start(loginparam, '', mainCallback)
        logger.debug(loginResult)
        if loginResult.ErrorCode != 0:
            logger.error("Choice quant -- login failed. ErrorCode is %d" % loginResult.ErrorCode)
            login_em()
    except Exception as ee:
        logger.error("error >>>", ee)
        logger.error("Choice quant -- login failed.")
        traceback.print_exc()
        login_em()
    else:
        logger.info("Choice quant -- login successful.")


def logout_em():
    try:
        logoutResult = c.stop()
        logger.debug(logoutResult)

    except Exception as ee:
        logger.error("error >>>", ee)
        logger.error("Choice quant -- logout failed")
        traceback.print_exc()
    else:
        logger.info("Choice quant -- logout successful")


def pre_exec(country: CountryCode):
    # EM has been obsoleted.
    # login_em()
    DataContext.initklz(country)
    data_context = DataContext()
    StrategyContext.initklz(data_context)
    loadsectors(data_context)
    return data_context


def post_exec(context: DataContext):
    # EM has been obsoleted.
    # updatedatabase(True, DataSource.EAST_MONEY)
    # logout_em()
    # FIXME: only for datasource AKShare
    # updatedatabase(True, DataSource.SNAPSHOT, context)
    updatedatabase(True, DataSource.EFINANCE)
    if getdbconn():
        getdbconn().cursor().close()
        getdbconn().close()
    logger.debug("PostgreSQL connection is closed")


def backtest(context: DataContext):
    if DataContext.iscountryChina():
        mins15 = 16
    if DataContext.iscountryUS():
        mins15 = 26
    base = mins15 * 2
    end_point = - mins15 - base
    start_point = - 1 - base

    results = {}
    for stockdata in context.data15mins.values():
        for key in stockdata.keys():
            data = stockdata.get(key)
            strategy_cross = CROSSUpMAAction(data)
            valid, result_tmp = strategy_cross.executeaction(startindex=start_point, endindex=end_point,
                                                             cross_period=context.cross_sma_period,
                                                             greater_period=context.greater_than_sma_period)
            if valid:
                if len(result_tmp) > 0:
                    # it is needed to reverse result_tmp because start point is last item.
                    reversed_result = result_tmp.iloc[::-1]
                    for row in reversed_result.itertuples(index=False):
                        if row[7]:
                            close_price = row[2]
                            times = 1.05
                            slot = -1
                            for i in range(-(end_point+mins15)):
                                if data.iloc[slot-i]['high'] >= close_price * times:
                                    ret_earning = True
                                    break
                            else:
                                ret_earning = False
                            results[key] = ret_earning
                            break
            else:
                print("strategy_cross_70 is failed on {}".format(key))
    print("符合条件的股票共计: %d" % len(results))
    win_stocks = []
    for index, value in results.items():
        if value:
            win_stocks.append(index)
    if len(results) == 0:
        return
    print("盈利的股票占比: {}%".format(len(win_stocks)/len(results)*100))
    print("盈利的股票是:")
    print(" ".join(win_stocks))
    '''
    login_em()
    loadsectors(context)
    logout_em()
    sector_selected = context.sectors.get(SectorUS.sector_201001, [])
    print("中概股共计: %d" % len(sector_selected))
    result_selected = {}
    for index in results:
        if (isinstance(sector_selected, list) and index in sector_selected) or index == sector_selected:
            result_selected[index] = results[index]

    print("中概股中符合条件的股票共计: %d" % len(result_selected))
    win_stocks = []
    for index, value in result_selected.items():
        if value:
            win_stocks.append(index)
    if len(result_selected) == 0:
        return
    print("盈利的股票占比: {}%".format(len(win_stocks) / len(result_selected) * 100))
    print("盈利的股票是:")
    print(" ".join(win_stocks))
    '''


if __name__ == '__main__':
    # ["创业板", "中小企业板", "主板A股", "主板", "科创板"]
    # DataContext.initklz(CountryCode.CHINA)
    # loaddata(["主板A股"], 2, datasource=DataSource.AK_SHARE)
    # loaddata(["主板A股"], 3, c_point=600216, datasource=DataSource.AK_SHARE)
    # login_em(isforcelogin=False)
    # loaddata(["科创板"], 2, datasource=DataSource.EAST_MONEY)
    # loaddata(DataContext.markets, 2, datasource=DataSource.EAST_MONEY, period=240, type_func=2)
    
    dcon = pre_exec(CountryCode.CHINA)
    # dcon = pre_exec(CountryCode.US)
    t = threading.Thread(target=handleresult, args=(dcon,))
    t.start()
    snapshot(dcon)
    time.sleep(60)
    post_exec(dcon)
    '''
    DataContext.initklz(CountryCode.CHINA)
    # updatedatabase(source=DataSource.EFINANCE)
    # loaddata(["主板", "科创板", "创业板", "中小企业板", "主板A股"], 2, datasource=DataSource.EFINANCE)
    loaddata(["主板A股"], 3, c_point=600446, datasource=DataSource.EFINANCE)
    # DataContext.country = CountryCode.CHINA
    # checksymbols()
    '''
    # DataContext.initklz(CountryCode.CHINA)
    # backtest(DataContext())
    # calconhistory(DataContext())
    # quantstrategies(DataContext())

    # login_em(isforcelogin=False)
    # logout_em()
    # updatedatabase(source=DataSource.EAST_MONEY)
    # loadsectorsfromEM()
    # reloaddata({})

    # DataContext.country = CountryCode.US
    # loaddata(["NASDAQ", "NYSE", "AMEX"], 2, datasource=DataSource.YAHOO)
    # loaddata(["NASDAQ"], 3, c_point='AMBA', datasource=DataSource.YAHOO)
