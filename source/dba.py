import os
import sqlite3
from cmn_function import raw_file_parser


class DataBaseAdmin:
    def __init__(self):
        self.db_name = "FX_Historical_Data.db"
        self.db_path = r"C:\1.PyProject\AlgoTrading\database"
        self.db = sqlite3.connect(os.path.join(self.db_path, self.db_name))
        self.cursor = self.db.cursor()
        self.result = []

    def update(self, file_list):
        parsed_f_lst = raw_file_parser(file_list)
        for f_name in parsed_f_lst:
            data_tuple_list = []
            with open(f_name, 'r') as src:
                for line in src:
                    str_list = line.strip().split(",")
                    data_tuple = tuple([str_list[0]] + map(int, line.strip().split(',')[1].split('.')) + map(int, str_list[2].split(":")) + map(float, str_list[3:-1]) + [str_list[-1]])
                    data_tuple_list.append(data_tuple)
            self.cursor.executemany("INSERT INTO M1_Data(Currency, Year, Month, Date, Hour, Minute, Open, High, Low, Close, ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data_tuple_list)
            self.db.commit()

    def select_intraday_peak_time(self, date_str):
        sqlcmd = "select Time as high_time from M1_Data where Date=%s and  High= (  select high from    ( select Currency, Open, max(High) as high," \
                 " min(Low), Close from M1_Data where Date=%s ) as summary ) Union select Time as low_time from M1_Data where Date=%s and Low= (  " \
                 "select low from    ( select Currency, Open, max(High), min(Low) as low, Close from M1_Data where Date=%s ) as summary ) " % (date_str, date_str, date_str, date_str)
        self.cursor.execute(sqlcmd)
        return self.cursor.fetchall()

    def get_profit_count_among_year(self, start_time, end_time, limit, stop):
        sqlcmd = "select month, date,  sum(Open) as Open, (max(high) - sum(Open)) * 10000 as high, (min(low) -  sum(Open)) * 10000 as low, " \
                 "(max(high) - min(low)) * 10000 as range from ( select month, date, hour, case hour when %d then Open else 0 end as open, high, low, range from Hourly_Table ) T " \
                 "where hour between %d and %d group by month, date having ((max(high) - sum(Open)) * 10000 > %d and (min(low) -  sum(Open)) * 10000 > %d and sum(Open) > 1) " \
                 "or ((max(high) - sum(Open)) * 10000 < %d and (min(low) -  sum(Open)) * 10000 < %d and sum(Open) > 0)" % (start_time, start_time, end_time, limit, 0 - stop, stop, 0 - limit)
        self.cursor.execute(sqlcmd)
        result = self.cursor.fetchall()
        return  len(result)

    def get_double_lose_count_among_year(self, start_time, end_time, limit, stop):
        sqlcmd = "select month, date,  sum(Open) as Open, (max(high) - sum(Open)) * 10000 as high, (min(low) -  sum(Open)) * 10000 as low, " \
                 "(max(high) - min(low)) * 10000 as range from ( select month, date, hour, case hour when %d then Open else 0 end as open, high, low, range from Hourly_Table ) T " \
                 "where hour between %d and %d group by month, date having ((max(high) - sum(Open)) * 10000 < %d and (max(high) - sum(Open)) * 10000 > %d  and (min(low) -  sum(Open)) * 10000 > %d and sum(Open) > 1) " \
                 "or ((max(high) - sum(Open)) * 10000 < %d and (min(low) -  sum(Open)) * 10000 > %d and (min(low) -  sum(Open)) * 10000 < %d and sum(Open) > 1) " % (start_time, start_time, end_time, limit, stop, 0-stop, stop, 0-limit, 0-stop)
        sqlcmd = "select  \
                            month, date, sum(Open) as Open,  \
                            (max(high) - sum(Open)) * 10000 as high,  \
                            (min(low) -  sum(Open)) * 10000 as low, \
                            (max(high) - min(low)) * 10000 as range  \
                        from  \
                            (  \
                                select  \
                                    month, date, hour,  \
                                    case hour when %d then Open else 0 end as open,  \
                                    high, low, range  \
                                from Hourly_Table ) T  \
                        where  \
                            hour between %d and %d  \
                        group by  \
                            month, date  \
                        having  \
                            ( \
                                (max(high) - sum(Open)) * 10000 < %d and  \
                                (max(high) - sum(Open)) * 10000 > %d and  \
                                (min(low) -  sum(Open)) * 10000 < %d and  \
                                sum(Open) > 1 \
                            )  \
                            or  \
                            ( \
                                (max(high) - sum(Open)) * 10000 > %d and  \
                                (min(low) -  sum(Open)) * 10000 < %d and  \
                                (min(low) -  sum(Open)) * 10000 > %d and  \
                                sum(Open) > 1 \
                            ) \
                            or \
                            ( \
                                (max(high) - sum(Open)) * 10000 < %d and  \
                                (max(high) - sum(Open)) * 10000 > %d and  \
                                (min(low) -  sum(Open)) * 10000 < %d and  \
                                (min(low) -  sum(Open)) * 10000 > %d and  \
                                sum(Open) > 1 \
                            )" % (start_time, start_time, end_time, limit, stop, 0 - limit,
                                  limit, 0 - stop, 0- limit, limit, stop, 0 - stop, 0- limit)
        self.cursor.execute(sqlcmd)
        result = self.cursor.fetchall()
        return  len(result)

    def get_overall_profit_count(self):
        stop = 28
        for start_time in range(0, 24):
            for end_time in range(start_time, 24):
                for profit in range(28, 40):
                    win_count = self.get_profit_count_among_year(start_time, end_time, stop + profit, stop)
                    double_lose_count = self.get_double_lose_count_among_year(start_time, end_time, stop + profit, stop)
                    self.result.append((start_time, end_time, win_count, double_lose_count, win_count * (profit) - double_lose_count * 2 * stop, stop, profit),)
        self.result.sort(key = lambda x: x[4], reverse=True)
        for x in range(0, 10):
            print self.result[x]

    def select_all_dates(self):
        sqlcmd = "select distinct Date from M1_Data"
        self.cursor.execute(sqlcmd)
        return self.cursor.fetchall()


    def close(self):
        self.db.close()



dba = DataBaseAdmin()
dba.get_overall_profit_count()
#dba.update([r"C:\1.PyProject\historicalDataAnalyzer\rawdata\m1_data\DAT_MT_EURUSD_M1_2016.csv"])
#date_list = dba.select_all_dates()
#with open('result.csv', 'w') as dst:
#    for date_tuple in date_list:
#        new_line = ""
#        for date_str in date_tuple:
#            records =  dba.select_intraday_peak_time(date_str)
#            new_line = ",".join([str(date_str), records[0][0], records[1][0]+"\n"])
#        dst.write(new_line)

dba.close()




