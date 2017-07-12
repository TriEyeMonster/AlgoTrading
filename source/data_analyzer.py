import time

class DataAnalyzer:
    def __init__(self, db, cursor):
        self.db = db
        self.cursor = cursor

    def parse_peak_time_result(self, f_name):
        line_no = 0
        with open(f_name, 'r') as src:
            for line_no, line_str in enumerate(src):
                str_list = line_str.strip().split(",")
                high_time = time.strptime(line_str[1], "%H:%M")
                low_time = time.strptime(line_str[2], "%H:%M")


