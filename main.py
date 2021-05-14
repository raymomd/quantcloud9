import pandas as pd
import akshare as ak
import yfinance as yf
import psycopg2
from sqlalchemy import create_engine
from numpy import (float64, nan, datetime64)
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


# 列名与数据对其显示
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
# pd.set_option('display.max_rows', None)


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

class CountryCode(enum.Enum):
    CHINA = 'cn'
    US = 'us'
    NONE = 'none'


class SectorCN(enum.Enum):
    # 000001 优选股关注1
    sector_000001 = '000001'
    # 007180 券商概念
    sector_007180 = '007180'
    # 007224 大飞机
    sector_007224 = '007224'
    # 007315 半导体
    sector_007315 = '007315'
    # 007205 国产芯片
    sector_007205 = '007205'
    # 007039 生物疫苗
    sector_007039 = '007039'
    # 007001 军工
    sector_007001 = '007001'
    # 007139 医疗器械
    sector_007139 = '007139'
    # 007146 病毒防治
    sector_007146 = '007146'
    # 007147 独家药品
    sector_007147 = '007147'
    # 007162 基因测序
    sector_007162 = '007162'
    # 007167 免疫治疗
    sector_007167 = '007167'
    # 007188 健康中国
    sector_007188 = '007188'
    # 007195 人工智能
    sector_007195 = '007195'
    # 007200 区块链
    sector_007200 = '007200'
    # 007206 新能源车
    sector_007206 = '007206'
    # 007212 生物识别
    sector_007212 = '007212'
    # 007218 精准医疗
    sector_007218 = '007218'
    # 007220 军民融合
    sector_007220 = '007220'
    # 007243 互联医疗
    sector_007243 = '007243'
    # 007246 体外诊断
    sector_007246 = '007246'
    # 007284 数字货币
    sector_007284 = '007284'
    # 007332 长寿药
    sector_007332 = '007332'
    # 007336 疫苗冷链
    sector_007336 = '007336'
    # 007339 肝素概念
    sector_007339 = '007339'
    # 014010018003 生物医药
    sector_014010018003 = '014010018003'
    # 004012003001 太阳能
    sector_004012003001 = '004012003001'
    # 015011003003 光伏
    sector_015011003003 = '015011003003'
    # 007371 低碳冶金
    sector_007371 = '007371'
    # 018001001002001 新能源设备与服务
    sector_018001001002001 = '018001001002001'
    # 007068 太阳能
    sector_007068 = '007068'
    # 007005 节能环保
    sector_007005 = '007005'
    # 007152 燃料电池
    sector_007152 = '007152'
    # 007307 HIT电池
    sector_007307 = '007307'
    # 007370 光伏建筑一体化
    sector_007370 = '007370'
    # 007369 碳化硅
    sector_007369 = '007369'
    # 007359 碳交易
    sector_0073259 = '007359'


sectornames_CN = {SectorCN.sector_000001: "优选股关注",
                  SectorCN.sector_007180: "券商概念",
                  SectorCN.sector_007224: "大飞机",
                  SectorCN.sector_007315: "半导体",
                  SectorCN.sector_007205: "国产芯片",
                  SectorCN.sector_007039: "生物疫苗",
                  SectorCN.sector_007001: "军工",
                  SectorCN.sector_007139: "医疗器械",
                  SectorCN.sector_007146: "病毒防治",
                  SectorCN.sector_007147: "独家药品",
                  SectorCN.sector_007162: "基因测序",
                  SectorCN.sector_007167: "免疫治疗",
                  SectorCN.sector_007188: "健康中国",
                  SectorCN.sector_007195: "人工智能",
                  SectorCN.sector_007200: "区块链",
                  SectorCN.sector_007206: "新能源车",
                  SectorCN.sector_007212: "生物识别",
                  SectorCN.sector_007218: "精准医疗",
                  SectorCN.sector_007220: "军民融合",
                  SectorCN.sector_007243: "互联医疗",
                  SectorCN.sector_007246: "体外诊断",
                  SectorCN.sector_007284: "数字货币",
                  SectorCN.sector_007332: "长寿药",
                  SectorCN.sector_007336: "疫苗冷链",
                  SectorCN.sector_007339: "肝素概念",
                  SectorCN.sector_014010018003: "生物医药",
                  SectorCN.sector_004012003001: "太阳能",
                  SectorCN.sector_015011003003: "光伏",
                  SectorCN.sector_007371: "低碳冶金",
                  SectorCN.sector_018001001002001: "新能源设备与服务",
                  SectorCN.sector_007068: "太阳能",
                  SectorCN.sector_007005: "节能环保",
                  SectorCN.sector_007152: "燃料电池",
                  SectorCN.sector_007307: "HIT电池",
                  SectorCN.sector_007370: "光伏建筑一体化",
                  SectorCN.sector_007369: "碳化硅",
                  SectorCN.sector_0073259: "碳交易"}


class SectorUS(enum.Enum):
    # 000001 自定义关注1
    sector_000001 = '000001'
    # 201001 全部中概股
    sector_201001 = '201001'


sectornames_US = {SectorUS.sector_000001: "优选股关注",
                  SectorUS.sector_201001: "中概股"}


# param: echo=True that is used to show each sql statement used in query
engine = create_engine("postgresql+psycopg2://Raymond:123123@localhost:5432/Raymond", encoding='utf-8')


class DataSource(enum.Enum):
    EAST_MONEY = 0
    AK_SHARE = 1
    YAHOO = 2


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


def inserttab(exchange: str, symbol: str, stock_df: pd.DataFrame, datasource: DataSource, period=15, transientdf: pd.DataFrame=None):
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
        elif datasource == DataSource.EAST_MONEY:
            stock_day = stock_df.index.tolist()
            header_o = 'OPEN'
            header_c = 'CLOSE'
            header_h = 'HIGH'
            header_l = 'LOW'
            header_v = 'VOLUME'
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
    stock_volume = stock_df[header_v]
    if period == 15:
        count: int = 0
        for each_time in stock_day:
            if DataContext.iscountryUS():
                csr.execute(statement_start + exchange + "_tbl (gid,crt_time,open,close,high,low,volume) " +
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + " do nothing;",
                            (str(symbol), str(each_time), "{:.4f}".format(stock_open[count]), "{:.4f}".format(stock_close[count]),
                            "{:.4f}".format(stock_high[count]), "{:.4f}".format(stock_low[count]), str(stock_volume[count])))
            elif DataContext.iscountryChina():
                csr.execute(statement_start + exchange + "_tbl (gid,crt_time,open,close,high,low,volume) " +
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + " do nothing;",
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
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + "_30 do nothing;",
                            (str(symbol), str(stock_day[next_idx]), "{:.4f}".format(open_value), "{:.4f}".format(close_value),
                            "{:.4f}".format(high_value), "{:.4f}".format(low_value), str(volume_value)))
            elif DataContext.iscountryChina():
                csr.execute(statement_start + exchange + "_tbl_30 (gid,crt_time,open,close,high,low,volume) " +
                            "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + "_30 do nothing;",
                            (str(symbol), str(stock_day[next_idx]), str(open_value), str(close_value),
                             str(high_value), str(low_value), str(volume_value)))
                transientdf.loc[len(transientdf)] = [str(symbol), open_value, close_value, high_value, low_value,
                                                     volume_value, stock_day[next_idx], nan]
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
        stock_volume = transientdf['volume']
        statement_start = "insert into china_"
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
                        "values (%s,%s,%s,%s,%s,%s,%s) on conflict on constraint time_key_" + exchange + "_60 do nothing;",
                        (str(symbol), str(stock_day[next_idx]), str(open_value), str(close_value),
                         str(high_value), str(low_value), str(volume_value)))
            count += 1
        conn.commit()
        logger.debug("%s - rows are %d for period 60 mins" % (symbol, count))


def insertdata(exchange: str, group: str, symbols: list, retried, datasource: DataSource, period: str = '15',
               adjust: str = "qfq"):
    exchange_group = ",".join([exchange, group])
    if DataContext.iscountryChina():
        for symbol_i in symbols:
            if datasource == DataSource.AK_SHARE:
                symbol_internal = group + str(symbol_i)
                stock_zh_df_tmp = ak.stock_zh_a_minute(symbol=symbol_internal, period=period, adjust=adjust)
            elif datasource == DataSource.EAST_MONEY:
                symbol_internal = ".".join([str(symbol_i), group])
                stock_zh_df_tmp = c.cmc(symbol_internal, "OPEN,HIGH,LOW,CLOSE,VOLUME,TIME",
                                        (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                                        datetime.datetime.today().strftime("%Y-%m-%d"),
                                        "AdjustFlag=1,RowIndex=2,Period=15,IsHistory=1,Ispandas=1")
                if isinstance(stock_zh_df_tmp, c.EmQuantData) and stock_zh_df_tmp.ErrorCode != 0:
                    logger.error("it is failed to get stock data for {} {} and error code is {} error message is {}".
                                 format(symbol_i, exchange_group, stock_zh_df_tmp.ErrorCode, stock_zh_df_tmp.ErrorMsg))
                    if stock_zh_df_tmp.ErrorMsg.find('service error') != -1 or \
                            stock_zh_df_tmp.ErrorCode == 10002011 or \
                            stock_zh_df_tmp.ErrorCode == 10002010 or \
                            stock_zh_df_tmp.ErrorCode == 10002004:
                        append_value(retried, exchange_group, symbol_i)

            if isinstance(stock_zh_df_tmp, pd.DataFrame):
                inserttab(exchange, symbol_i, stock_zh_df_tmp, datasource)
                tmp_df = pd.DataFrame(columns=columns)
                inserttab(exchange, symbol_i, stock_zh_df_tmp, datasource, period=30, transientdf=tmp_df)
                inserttab(exchange, symbol_i, stock_zh_df_tmp, datasource, period=60, transientdf=tmp_df)
    elif DataContext.iscountryUS():
        for symbol_i in symbols:
            stock_us_df_tmp = yf.download(tickers=symbol_i, auto_adjust=True, period="10d", interval="15m")
            if isinstance(stock_us_df_tmp, pd.DataFrame):
                inserttab(exchange, symbol_i, stock_us_df_tmp, datasource)
                inserttab(exchange, symbol_i, stock_us_df_tmp, datasource, period=30)


def insertdata_continue(exchange: str, group: str, symbols: list, c_point: str, retried, datasource: DataSource,
                        period: str = '15', adjust: str = "qfq"):
    pos = (pd.Series(symbols) == c_point).argmax() + 1
    insertdata(exchange, group, symbols[pos:], retried, datasource)


def loaddatalocked(indicator: str, exchange: str, symbols: list, operation: int, datasource: DataSource,
                   c_point: str, retried={}, period=15):
    group = stock_group[indicator]
    if operation == 1:
        createtable(symbols, group, period)
    elif operation == 2:
        insertdata(group, exchange, symbols, retried, datasource)
    elif operation == 3:
        insertdata_continue(group, exchange, symbols, c_point, retried, datasource)
    elif operation == 4:
        droptable(symbols, group)


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


def loaddata(indicators, operation: int, c_point='', datasource: DataSource = DataSource.AK_SHARE, period=15):
    retriedStocks = {}
    if datasource == DataSource.EAST_MONEY:
        login_em()
    try:
        loaddatainternal(indicators, operation, c_point, retriedStocks, datasource, period)
        if datasource == DataSource.EAST_MONEY:
            reloaddata(retriedStocks)
    finally:
        if datasource == DataSource.EAST_MONEY:
            logout_em()
        if getdbconn():
            getdbconn().cursor().close()
            getdbconn().close()
        logger.debug("PostgreSQL connection is closed")


def loaddatainternal(indicators, operation: int, c_point='', retried={}, datasource: DataSource = DataSource.AK_SHARE, period=15):
    try:
        for indicator in indicators:
            group, symbols = selectgroup(indicator)
            loaddatalocked(indicator, group, symbols, operation, datasource, c_point, retried, period)
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


class SMAAction(ActionBase):

    def __init__(self, startindex: int, endindex: int, period: int, data: pd.DataFrame):
        super().__init__(data)
        self.__startindex = startindex
        self.__endindex = endindex
        self.__period = period

    def __sma(self, fn_ticker):
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
        return self.__sma(kwargs['fn_ticker'])


class CROSSUpSMAAction(ActionBase):
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
        sma_cross = SMAAction(index_s, index_e, _cross_period, self._data)
        valid_cross, result_cross = sma_cross.executeaction(fn_ticker=self.close_ticker)
        sma_greater = SMAAction(index_s, index_e, _greater_period, self._data)
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

    def rsv(self, index):
        ret = 0
        valid1, result_llv = self.__llvaction.executeaction(fn_ticker=self.low_ticker, index_c=index)
        if not valid1:
            return valid1, ret
        valid2, result_hhv = self.__hhvaction.executeaction(fn_ticker=self.high_ticker, index_c=index)
        if not valid2:
            return valid2, ret
        ret = (self.close_ticker(index) - result_llv) / (result_hhv - result_llv) * 100
        return True, ret

    def kvalue(self, index):
        valid = False
        ret = None
        if index < self.__rsv_period - 1 or index >= len(self.__k_v):
            logger.error("index is invalid in kvalue", index)
        else:
            ret = self.__k_v[index]
            if ret is not None:
                valid = True
        return valid, ret

    def sma(self, fnf, n, m, index, values):
        if index < self.__rsv_period - 1:
            logger.error("index must be greater than %d for KD in sma", self.__rsv_period - 1)
            return False
        valid_r, value_r = fnf(index)
        k_t_v = False
        if valid_r:
            if index == self.__rsv_period -1:
                values[index] = (value_r*m + 50*(n-m))/n
                k_t_v = True
            else:
                index_p = index - 1
                k_t_v = self.sma(fnf, n, m, index_p, values)
                if k_t_v:
                    values[index] = (value_r*m + values[index_p]*(n-m))/n
        return valid_r and k_t_v

    def executeaction(self, **kwargs):
        index = len(self._data.index)-1
        ret_v = self.sma(self.rsv, self.__k_period, 1, index, self.__k_v)
        ret_v &= self.sma(self.kvalue, self.__d_period, 1, index, self.__d_v)
        return ret_v, self.__k_v, self.__d_v


class CROSSUpKDAction(ActionBase):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)

    def __comparevalue(self, k_cur: float64, d_cur: float64,
                       k_pre: float64, d_pre: float64,
                       k_pre2: float64, d_pre2: float64):
        condition = k_cur > k_pre and k_cur > d_cur and k_pre <= d_pre
        if k_pre2 is not None and d_pre2 is not None:
            condition |= k_cur > k_pre == d_pre and k_pre2 <= d_pre2
        return condition

    def executeaction(self, **kwargs):
        occurnaces = kwargs['occurance_time']
        rsv_p = kwargs['rsv_period']
        k_p = kwargs['k_period']
        d_p = kwargs['d_period']
        c_v = kwargs['crossvalue']
        ret_valid = False
        ret_value = pd.DataFrame(columns=columns)
        kd_indicator = KDAction(self._data, rsv_p, k_p, d_p)
        for time_stamp in occurnaces:
            valid1, k_v, d_v = kd_indicator.executeaction()
            if valid1:
                index_c = self.getindex(time_stamp)
                index_p, index_p2 = (index_c-1, index_c-2)
                if k_v[index_c] is None or d_v[index_c] is None or index_p < 0 or k_v[index_p] is None or d_v[index_p] is None:
                    continue
                k_c_v = k_v[index_c]
                d_c_v = d_v[index_c]
                k_p_v = k_v[index_p]
                d_p_v = d_v[index_p]
                k_p2_v = d_p2_v = None
                if index_p2 >= 0:
                    k_p2_v = k_v[index_p2]
                    d_p2_v = d_v[index_p2]
                ret_valid = True
                if self.__comparevalue(k_c_v, d_c_v, k_p_v, d_p_v, k_p2_v, d_p2_v):
                    if c_v[0]:
                        if (not (k_c_v > c_v[1] and k_p_v > c_v[1]) and not (d_c_v > c_v[1] and d_p_v > c_v[1])) \
                                or (k_p2_v is not None and d_p2_v is not None and k_p_v < c_v[1] and d_p_v < c_v[1]):
                            row = self._data.loc[time_stamp]
                            ret_value.loc[len(ret_value)] = [row['gid'], row['open'], row['close'],
                                                             row['high'], row['low'], row['volume'],
                                                             time_stamp, True]
                    else:
                        row = self._data.loc[time_stamp]
                        ret_value.loc[len(ret_value)] = [row['gid'], row['open'], row['close'],
                                                         row['high'], row['low'], row['volume'],
                                                         time_stamp, True]
        return ret_valid, ret_value


class EntangleKDACtion(ActionBase):
    def __init__(self, data: pd.DataFrame):
        super().__init__(data)

    def __comparevalue(self, kd_results: list, periods: int):
        ret = True
        for i in range(periods):
            k_v = kd_results[i][0]
            d_v = kd_results[i][1]
            if k_v != 0 and d_v != 0:
                if k_v > d_v:
                    ret &= abs((k_v - d_v) / k_v) <= 0.15
                else:
                    ret &= abs((d_v - k_v) / d_v) <= 0.15
            else:
                if k_v == d_v:
                    pass
                elif k_v == 0:
                    ret &= abs(d_v) < 2
                else:
                    ret &= abs(k_v) < 2
        return ret

    def executeaction(self, **kwargs):
        occurnaces = kwargs['occurance_time']
        rsv_p = kwargs['rsv_period']
        k_p = kwargs['k_period']
        d_p = kwargs['d_period']
        c_v = kwargs['crossvalue']
        periods = kwargs['periods']
        ret_valid = False
        ret_value = pd.DataFrame(columns=columns)
        kd_indicator = KDAction(self._data, rsv_p, k_p, d_p)
        for time_stamp in occurnaces:
            kd_results = []
            valid1, k_v, d_v = kd_indicator.executeaction()
            if valid1:
                ret_valid = True
                index_c = self.getindex(time_stamp)
                for i in range(periods):
                    index_t = index_c - i
                    if index_t < 0:
                        break;
                    k_v_t = k_v[index_t]
                    d_v_t = d_v[index_t]
                    if k_v_t is not None and d_v_t is not None:
                        if c_v[0]:
                            if k_v_t <= c_v[1] and d_v_t <= c_v[1]:
                                kd_results.append((k_v_t, d_v_t))
                            else:
                                break
                        else:
                            kd_results.append((k_v_t, d_v_t))
                    else:
                        break
                else:
                    if self.__comparevalue(kd_results, periods):
                        row = self._data.loc[time_stamp]
                        ret_value.loc[len(ret_value)] = [row['gid'], row['open'], row['close'],
                                                         row['high'], row['low'], row['volume'],
                                                         time_stamp, True]

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
        occurnaces = kwargs['occurance_time']
        obv_p = kwargs['obv_period']
        obv_a_p = kwargs['obv_a_period']
        ret_valid = False
        ret_value = pd.DataFrame(columns=columns)
        obv_indicator = OBVAction(self._data, obv_p)
        oa = occurnaces.array
        for time_stamp_original in oa:
            cur_index = self.getindex(time_stamp_original)
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


class DataContext:
    country = CountryCode.NONE
    limitfordatafetched = 0
    limitfordatafetched_30 = 0
    limitfordatafetched_60 = 0
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
    strategy1_2 = 'strategy1and2'

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

    @classmethod
    def initklz(cls, country_param: CountryCode):
        DataContext.country = country_param
        if DataContext.iscountryChina():
            DataContext.limitfordatafetched = 160
            DataContext.limitfordatafetched_30 = 120
            DataContext.limitfordatafetched_60 = 80
            DataContext.markets = ["科创板", "创业板", "中小企业板", "主板A股", "主板"]
            DataContext.marketopentime = datetime.time(hour=9, minute=30)
            DataContext.marketclosetime = datetime.time(hour=15)
            DataContext.marketbreakstarttime = datetime.time(hour=11, minute=30)
            DataContext.marketbreakstoptime = datetime.time(hour=13)
            DataContext.dir_name = os.path.join(r'./result_strategy/cn', datetime.datetime.today().strftime('%Y%m%d'))
        elif DataContext.iscountryUS():
            DataContext.limitfordatafetched = 260
            DataContext.limitfordatafetched_30 = 195
            DataContext.limitfordatafetched_60 = 21
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
        self.totalresult = {DataContext.strategy1: {}, DataContext.strategy2: {}, DataContext.strategy3: {}}
        self.sectors = {}

        logger.debug("Initialization of context is done.")

    def preparedata(self, indicator: str):
        if indicator in {"中小企业板", "创业板", "主板"}:
            header = "公司代码"
            exchange = 'SZ'
            tablename_prefix = 'china_' + stock_group[indicator] + '_tbl_'
            tablename_prefix_30 = 'china_' + stock_group[indicator] + '_tbl_30_'
            tablename_prefix_60 = 'china_' + stock_group[indicator] + '_tbl_60_'
        elif indicator in {"科创板", "主板A股"}:
            header = 'SECURITY_CODE_A'
            exchange = 'SH'
            if indicator == "科创板":
                tablename_prefix = 'china_tbl_'
                tablename_prefix_30 = 'china_' + stock_group[indicator] + '_tbl_30_'
                tablename_prefix_60 = 'china_' + stock_group[indicator] + '_tbl_60_'
            elif indicator == "主板A股":
                tablename_prefix = 'china_' + stock_group[indicator] + '_tbl_'
                tablename_prefix_30 = 'china_' + stock_group[indicator] + '_tbl_30_'
                tablename_prefix_60 = 'china_' + stock_group[indicator] + '_tbl_60_'
        elif indicator in {"NASDAQ", "NYSE", "AMEX"}:
            header = 'SECURITY_CODE_A'
            tablename_prefix = 'us_' + stock_group[indicator] + '_tbl_'
            tablename_prefix_30 = 'us_' + stock_group[indicator] + '_tbl_30_'
            tablename_prefix_60 = 'china_' + stock_group[indicator] + '_tbl_60_'

        tmp_symbol_l = []
        tmp_symbol_exchange_l = []
        tmp_data = StockData()
        tmp_data.sector = stock_group[indicator]
        tmp_data_30 = StockData()
        tmp_data_30.sector = stock_group[indicator]
        tmp_data_60 = StockData()
        tmp_data_60.sector = stock_group[indicator]

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
                table_name = tablename_prefix + symbol
                tmp_data.update(symbol, getdatafromdatabase(table_name, DataContext.limitfordatafetched))
                table_name_30 = tablename_prefix_30 + symbol
                tmp_data_30.update(symbol, getdatafromdatabase(table_name_30, DataContext.limitfordatafetched_30))
                if DataContext.iscountryChina():
                    table_name_60 = tablename_prefix_60 + symbol
                    tmp_data_60.update(symbol, getdatafromdatabase(table_name_60, DataContext.limitfordatafetched_60))

                # make up data based on interval of 30 mins
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
            elif indicator == "创业板":
                self.symbols_l_startup = tmp_symbol_l
                self.symbols_exchange_l_startup = tmp_symbol_exchange_l
                self.China_startup_15 = tmp_data
                self.China_startup_30 = tmp_data_30
                self.China_startup_60 = tmp_data_60
            elif indicator == "科创板":
                self.symbols_l_tech_startup = tmp_symbol_l
                self.symbols_exchange_l_tech_startup = tmp_symbol_exchange_l
                self.China_tech_startup_15 = tmp_data
                self.China_tech_startup_30 = tmp_data_30
                self.China_tech_startup_60 = tmp_data_60
            elif indicator == "主板A股":
                self.symbols_l_sh_a = tmp_symbol_l
                self.symbols_exchange_l_sh_a = tmp_symbol_exchange_l
                self.China_sh_a_15 = tmp_data
                self.China_sh_a_30 = tmp_data_30
                self.China_sh_a_60 = tmp_data_60
            elif indicator == "主板":
                self.symbols_l_sz_a = tmp_symbol_l
                self.symbols_exchange_l_sz_a = tmp_symbol_exchange_l
                self.China_sz_a_15 = tmp_data
                self.China_sz_a_30 = tmp_data_30
                self.China_sz_a_60 = tmp_data_60
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


def loadsectors(context: DataContext):
    date_t = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
    if DataContext.iscountryChina():
        sectors = SectorCN
    elif DataContext.iscountryUS():
        sectors = SectorUS
    for sector_i in sectors:
        if sector_i == sectors.sector_000001:
            append_value(context.sectors, sector_i, [str(code).zfill(6) for code in DataContext.code_spotlighted])
        else:
            data = c.sector(sector_i.value, date_t)
            if data.ErrorCode != 0:
                logger.debug("request sector %s Error, %s" % (sector_i.value, data.ErrorMsg))
            else:
                for code in data.Data:
                    code_l = code.split(".")
                    if len(code_l) > 1:
                        append_value(context.sectors, sector_i, code_l[0])


def snapshot(context: DataContext):
    # 1) start retrieving data once market is open
    # 2) retrieve snapshot of stocks depending on country every other 3 seconds according to limitation
    # 3) update stock data in context
    # 4) calculate indicators based on newly frenched stock data
    # 5) send result to another thread to handle
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
    current_time = datetime.datetime.now()
    if DataContext.iscountryChina():
        opentime = datetime.datetime.combine(datetime.date(year=current_time.year, month=current_time.month,
                                                           day=current_time.day),
                                             context.marketopentime)
        closetime = datetime.datetime.combine(datetime.date(year=current_time.year, month=current_time.month,
                                                            day=current_time.day),
                                              context.marketclosetime)

        breakstarttime = datetime.datetime.combine(datetime.date(year=current_time.year, month=current_time.month,
                                                                 day=current_time.day),
                                                   context.marketbreakstarttime)
        breakstoptime = datetime.datetime.combine(datetime.date(year=current_time.year, month=current_time.month,
                                                                day=current_time.day),
                                                  context.marketbreakstoptime)
    elif DataContext.iscountryUS():
        opentime = datetime.datetime.combine(datetime.date(year=current_time.year, month=current_time.month,
                                                           day=current_time.day),
                                             context.marketopentime)
        closetime = datetime.datetime.combine(datetime.date(year=current_time.year, month=current_time.month,
                                                            day=current_time.day),
                                              context.marketclosetime)

    target_time = datetime.timedelta(days=0, hours=0, minutes=0, seconds=0)

    symbols_exchange = []
    for sector in context.markets:
        symbols_exchange += context.symbols_exchange[stock_group[sector]]
    symbols_original_len = len(symbols_exchange)
    symbols_tmp = set(symbols_exchange)
    symbols_tmp.difference_update(DataContext.invalid_stock_codes)
    symbols_exchange = list(symbols_tmp)

    def updatestockdata(stockdata: pd.DataFrame, isnewrow: bool = False):
        def getrecordtime(period: int):
            if period == 15 or period == 30:
                slot = current_time.minute // period + 1
                if slot == 60 // period:
                    recordtime = datetime.datetime.combine(
                        datetime.date(year=current_time.year, month=current_time.month,
                                      day=current_time.day),
                        datetime.time(hour=current_time.hour + 1))
                else:
                    recordtime = datetime.datetime.combine(
                        datetime.date(year=current_time.year, month=current_time.month,
                                      day=current_time.day),
                        datetime.time(hour=current_time.hour, minute=slot * period))
            elif period == 60:
                if DataContext.iscountryChina():
                    if (current_time - opentime) >= target_time and (breakstarttime - current_time) >= target_time:
                        slot = (current_time - opentime).seconds // (period * 60) + 1
                        recordtime = datetime.datetime.combine(
                            datetime.date(year=current_time.year, month=current_time.month,
                                          day=current_time.day),
                            datetime.time(hour=opentime.hour + slot, minute=opentime.minute))
                    else:
                        slot = (current_time - breakstoptime).seconds // (period * 60) + 1
                        recordtime = datetime.datetime.combine(
                            datetime.date(year=current_time.year, month=current_time.month,
                                          day=current_time.day),
                            datetime.time(hour=breakstoptime.hour + slot))
                elif DataContext.iscountryUS():
                    if target_time <= (closetime - current_time) <= datetime.timedelta(days=0, hours=0, minutes=30,
                                                                                       seconds=0):
                        recordtime = datetime.datetime.combine(
                            datetime.date(year=current_time.year, month=current_time.month,
                                          day=current_time.day),
                            datetime.time(hour=closetime.hour))

                    else:
                        slot = (current_time - opentime).seconds // (period * 60) + 1
                        recordtime = datetime.datetime.combine(
                            datetime.date(year=current_time.year, month=current_time.month,
                                          day=current_time.day),
                            datetime.time(hour=opentime.hour + slot, minute=opentime.minute))

            return recordtime

        def sumvolume(size_p: int, start: int, dataset: pd.DataFrame):
            sum_volume = 0
            try:
                for j in range(size_p):
                    if start < 0:
                        sum_volume += dataset.iloc[start - j]['volume']
                    else:
                        sum_volume += dataset.iloc[start + j]['volume']
            except Exception as ee:
                # traceback.print_exc()
                logger.error("Symbol error occurred with {} error message is {}".format(dataset.iloc[-1]['gid'], ee))
            return sum_volume

        def updateexistingrow(firstk: bool, barcounter: int, dataset: pd.DataFrame):
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
                index_s = str(index)
                sector_code = stock_group[sector_usd]
                if index_s in context.symbols_exchange[sector_code]:
                    if DataContext.iscountryChina():
                        symbol_idx = index_s[:-3]
                    elif DataContext.iscountryUS():
                        symbol_idx = index_s[:-2]
                    tmpdata = context.data15mins[sector_code].get(symbol_idx)
                    tmpdata_30 = context.data30mins[sector_code].get(symbol_idx)
                    tmpdata_60 = context.data60mins[sector_code].get(symbol_idx)
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
                    else:
                        updateexistingrow(isfirstk_15, barcounter_15, tmpdata)
                        updateexistingrow(isfirstK_30, barcounter_30, tmpdata_30)
                        updateexistingrow(isfirstK_60, barcounter_60, tmpdata_60)
                    break

    while True:
        current_time = datetime.datetime.now()
        if DataContext.iscountryChina():
            timecondition = (((current_time - opentime) >= target_time and (breakstarttime - current_time) >= target_time)
                or ((current_time - breakstoptime) >= target_time and (closetime - current_time) >= target_time))
        elif DataContext.iscountryUS():
            timecondition = (((current_time - opentime) >= target_time) and ((closetime - current_time) >= target_time))

        if timecondition:
            # 1) french data
            logger.debug("totally scan %d stocks but the number of original stock codes is %d" %
                         (len(symbols_exchange), symbols_original_len))
            stock_data = csqsnapshot_t(symbols_exchange, "NOW,VOLUME", "Ispandas=1")
            if not isinstance(stock_data, c.EmQuantData):
                if not isinstance(stock_data, pd.DataFrame):
                    logger.debug("request csqsnapshot Error at {} ".format(current_time) +
                                 "because type of return value is not DataFrame")
                    time.sleep(5)
                    continue
                # 2) update stock data in context
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
                    if target_time <= (closetime - current_time) <= datetime.timedelta(days=0, hours=0, minutes=30, seconds=0):
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
                    updatestockdata(stock_data, True)
                else:
                    updatestockdata(stock_data)
                logger.debug("update stock data in context")
                # 3) calculate indicators
                logger.debug("run 3 strategies")
                try:
                    result = {current_time: quantstrategies(context)}
                except Exception as ee:
                    logger.error("It is failed to execute quantitative strategies. error >>>", ee)
                    traceback.print_exc()

                else:
                    logger.info("execute quantitative strategies successfully.")
                context.queue.put(result)
                logger.debug("send result to another thread to handle and sleep")
                if closetime - current_time > datetime.timedelta(minutes=5):
                    logger.debug("start to sleep with 3 minutes")
                    time.sleep(180)
                    logger.debug("sleep is done with 3 minutes")
                else:
                    logger.debug("start to sleep with 45 seconds")
                    time.sleep(45)
                    logger.debug("sleep is done with 45 minutes")
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
        elif (current_time - closetime) >= target_time:
            logger.debug("market is closed so that snapshot quits")
            context.queue.put(ProcessStatus.STOP)
            print("time windows for 15 mins:")
            print(time_windows_15)
            print("time windows for 30 mins:")
            print(time_windows_30)
            print("total number of retrieving data is %d" % fetchdatacounter)
            break
        else:
            logger.debug("market is not open or break is ongoing so that await. Now is {}".format(current_time))
            time.sleep(10)

# TODO refactor below codes with command-chain pattern
@time_measure
def quantstrategies(context: DataContext):
    totalresultdata = {}
    for sector_usd in context.markets:
        resultdata = {}
        sector_tmp = stock_group[sector_usd]
        for symbol_tmp in context.symbols[sector_tmp]:
            results = {}
            sma_cross = CROSSUpSMAAction(context.data15mins[sector_tmp].get(symbol_tmp))
            valid, result_tmp = sma_cross.executeaction(startindex=context.start_i, endindex=context.end_i,
                                                        cross_period=context.cross_sma_period,
                                                        greater_period=context.greater_than_sma_period)
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

                    kd_cross = CROSSUpKDAction(context.data30mins[sector_tmp].get(symbol_tmp))
                    valid, result_tmp = kd_cross.executeaction(occurance_time=time_sequence,
                                                               rsv_period=context.rsv_period,
                                                               k_period=context.k_period,
                                                               d_period=context.d_period, crossvalue=(False, 0))
                    if valid:
                        # FIXME
                        '''
                        if len(result_tmp) > 0:
                            obv_up = OBVUpACTION(context.data30mins[sector_tmp].get(symbol_tmp))
                            valid, result_tmp = obv_up.executeaction(occurance_time=result_tmp['time'],
                                                                     obv_period=context.obv_period,
                                                                     obv_a_period=context.obv_a_period)
                            if valid:
                                if len(result_tmp) > 0:
                                    results[DataContext.strategy1] = result_tmp
                                    resultdata[symbol_tmp] = results
                            else:
                                logger.error("strategy_obv_up_30 is failed on {}".format(symbol_tmp))
                        '''
                        if len(result_tmp) > 0:
                            results[DataContext.strategy1] = result_tmp
                            resultdata[symbol_tmp] = results
                    else:
                        logger.error("strategy_cross_kd_30 is failed on {}".format(symbol_tmp))
            else:
                logger.error("strategy_cross_70 is failed on {}".format(symbol_tmp))

            dataset_60 = context.data60mins[sector_tmp].get(symbol_tmp)
            if len(dataset_60) == 0:
                continue

            kd_cross_value = CROSSUpKDAction(dataset_60)
            valid_60, result_tmp_60 = kd_cross_value.executeaction(occurance_time=[dataset_60.index[-1]],
                                                                   rsv_period=context.rsv_period,
                                                                   k_period=context.k_period,
                                                                   d_period=context.d_period, crossvalue=(True, 30))
            if valid_60:
                if len(result_tmp_60) > 0:
                    results[DataContext.strategy2] = result_tmp_60
                    resultdata[symbol_tmp] = results
            else:
                logger.error("strategy_cross_kd_60 is failed on {}".format(symbol_tmp))

            kd_entangle_value = EntangleKDACtion(dataset_60)
            valid_60_entangle, result_entangle_60 = kd_entangle_value.executeaction(occurance_time=[dataset_60.index[-1]],
                                                                                    rsv_period=context.rsv_period,
                                                                                    k_period=context.k_period,
                                                                                    d_period=context.d_period,
                                                                                    crossvalue=(True, 30), periods=4)
            if valid_60_entangle:
                if len(result_entangle_60) > 0:
                    results[DataContext.strategy3] = result_entangle_60
                    resultdata[symbol_tmp] = results
            else:
                logger.error("strategy_entangle_kd_60 is failed on {}".format(symbol_tmp))

        totalresultdata[sector_tmp] = resultdata
    return totalresultdata


# the function runs in a separate thread
@time_measure
def handleresult(context: DataContext):
    # don't output a file including all of symbols and that can be imported by 365 as favor stocks
    # until figure out blk file format
    # simply output sector and symbol

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
            sendemail(subject_e1, content_e1, 'wsx_dna@sina.com')
            # sendemail(subject_e1, content_e1, 'stocash2021@163.com')
            # if DataContext.iscountryChina():
            #    sendemail(subject_e2, content_e2, 'wsx_dna@sina.com')
            context.sendemailtime = time_cur


class CalcResult:
    def __init__(self, ctime, isvgreater: bool):
        self.cross_time = ctime
        self.isgreater_v = isvgreater


# TODO remove duplicate item from totalreuslt
def mergeresult(context: DataContext, result_transient, ishistory: bool = False):
    def assembleFunc(symbol, strategy: str):
        symbol_s = str(symbol)
        symbols[strategy].append(symbol_s)
        if ishistory:
            append_value(context.totalresult[strategy], symbol_s, CalcResult(row[6], row[7]))
        else:
            append_value(context.totalresult[strategy], symbol_s, CalcResult(time_result, row[7]))

    symbols = {DataContext.strategy1: [], DataContext.strategy2: [], DataContext.strategy3: []}
    for time_result, result in result_transient.items():
        keytime = time_result
        for index, value in result.items():
            for index_1, value_1 in value.items():
                for index_2, value_2 in value_1.items():
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

    result_c_s1 = set(symbols[DataContext.strategy1])
    result_h_s1 = set(context.totalresult[DataContext.strategy1].keys()) - result_c_s1
    logger.info("%d symbols found with strategy 1 at %s" % (len(result_c_s1), keytime))
    result_c_s2 = set(symbols[DataContext.strategy2])
    result_h_s2 = set(context.totalresult[DataContext.strategy2].keys()) - result_c_s2
    logger.info("%d symbols found with strategy 2 at %s" % (len(result_c_s2), keytime))
    result_c_s3 = set(symbols[DataContext.strategy3])
    result_h_s3 = set(context.totalresult[DataContext.strategy3].keys()) - result_c_s3
    logger.info("%d symbols found with strategy 3 at %s" % (len(result_c_s3), keytime))
    result_c_s1_2 = result_c_s1.intersection(result_c_s2).union(result_c_s1.intersection(result_h_s2))
    result_h_s1_2 = result_h_s1.intersection(result_h_s2).union(result_h_s1.intersection(result_c_s2))
    logger.info("%d symbols found with strategy 1 and 2 at %s" % (len(result_c_s1_2), keytime))
    ret = {keytime: {DataContext.strategy3: [result_c_s3, result_h_s3],
                     DataContext.strategy1_2: [result_c_s1_2, result_h_s1_2],
                     DataContext.strategy1: [result_c_s1, result_h_s1],
                     DataContext.strategy2: [result_c_s2, result_h_s2]}}
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
            sectornames = sectornames_CN
            spotlightedsector = sectornames[SectorCN.sector_000001]
            print_order = [spotlightedsector]
        else:
            sectornames = sectornames_US
            spotlightedsector = sectornames[SectorUS.sector_000001]
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
                        str3 = ",".join(list_tmp)
                        str_p = " ".join(list_tmp)
                    else:
                        str3 = ",".join(value_s_o)
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
                if strategy_t == DataContext.strategy3:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略3 - 预警条件为:\r\n"
                    str101 += "  1. KD指标在60分钟周期至少持续纠缠4个周期且小于30\r\n\r\n"
                elif strategy_t == DataContext.strategy1:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略1 - 预警条件为:\r\n"
                    str101 += "  1. 收盘价在15分钟周期上穿70均线\r\n"
                    str101 += "  2. 成交量在15分钟周期大于80均线\r\n"
                    str101 += "  3. KD指标在30分钟周期形成金叉\r\n\r\n"
                    # FIXME
                    # str101 += "  4. OBV指标在30分钟周期大于零且大于30天均值\r\n\r\n"
                elif strategy_t == DataContext.strategy2:
                    str101 = "\r\n\r\n\r\n\r\n\r\n策略2 - 预警条件为:\r\n"
                    str101 += "  1. 收盘价在60分钟周期不大于50\r\n"
                    str101 += "  2. KD指标在60分钟周期形成金叉且金叉小于30\r\n\r\n"
                elif strategy_t == DataContext.strategy1_2:
                    str101 = "\r\n\r\n\r\n\r\n\r\n同时满足策略1和策略2的预警条件:\r\n\r\n"
                content_tmp, content_em_tmp = output(str101)
                emailcontent += content_tmp
                emailcontent_em += content_em_tmp

    return filename, emailcontent, filename_em, emailcontent_em


def sendemail(email_subject: str, email_content: str, recipient: str):
    msg = EmailMessage()
    msg["From"] = 'wsx_dna@sina.com'
    msg["To"] = recipient
    msg["Subject"] = email_subject
    msg.set_content(email_content)
    try:
        with smtplib.SMTP_SSL("smtp.sina.com", 465) as smtp:
            smtp.login("wsx_dna@sina.com", "f7556624333b77f3")
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


def updatedatabaselocked(board: str):
    logger.debug("The board data is downloaded for is {} and native thread id is {} and thread ident is {}".
                 format(board, threading.get_native_id(), threading.get_ident()))
    retriedstocks = {}
    loaddatainternal([board], 2, retried=retriedstocks, datasource=DataSource.EAST_MONEY)
    reloaddata(retriedstocks)
    logger.debug("Download data is done for the board {} and thread id is {} and thread ident is {}".
                 format(board, threading.get_native_id(), threading.get_ident()))


def updatedatabase():
    timedelta = datetime.timedelta(minutes=10)
    today = datetime.datetime.today()
    time_download = datetime.datetime.combine(datetime.date(year=today.year, month=today.month, day=today.day),
                                              datetime.time(hour=17, minute=30))
    while True:
        if (datetime.datetime.now() - time_download) > timedelta:
            break
        else:
            time.sleep(600)

    if DataContext.iscountryChina():
        threads = []
        for market in DataContext.markets:
            thread = threading.Thread(target=updatedatabaselocked, args=(market,))
            thread.start()
            threads.append(thread)
        for i in range(len(threads)):
            threads[i].join()
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
            logger.error("Choice quant -- login failed.")
            exit()
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
    login_em()
    DataContext.initklz(country)
    data_context = DataContext()
    loadsectors(data_context)
    return data_context


def post_exec():
    updatedatabase()
    logout_em()
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
            strategy_cross = CROSSUpSMAAction(data)
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
    # DataContext.initklz(CountryCode.CHINA)
    # loaddata(DataContext.markets, 2, datasource=DataSource.AK_SHARE)
    # loaddata(["创业板"], 3, c_point=300470, datasource=DataSource.EAST_MONEY)
    # loaddata(["创业板"], 2, datasource=DataSource.EAST_MONEY)

    dcon = pre_exec(CountryCode.CHINA)
    # dcon = pre_exec(CountryCode.US)
    t = threading.Thread(target=handleresult, args=(dcon,))
    t.start()
    snapshot(dcon)
    time.sleep(60)
    post_exec()

    # DataContext.country = CountryCode.CHINA
    # checksymbols()

    # DataContext.initklz(CountryCode.CHINA)
    # backtest(DataContext())
    # calconhistory(DataContext())
    # quantstrategies(DataContext())
    '''
    login_em()
    DataContext.initklz(CountryCode.CHINA)
    updatedatabase()
    logout_em()
    '''
    # DataContext.country = CountryCode.US
    # loaddata(["NASDAQ", "NYSE", "AMEX"], 2, datasource=DataSource.YAHOO)
    # loaddata(["NASDAQ"], 3, c_point='AMBA', datasource=DataSource.YAHOO)
