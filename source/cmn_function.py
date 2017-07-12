import os
import shutil


def check_path(t_path):
    if not os.path.exists(t_path):
        os.makedirs(t_path)
    return


def raw_file_parser(file_list):
    return_file_list = []
    for s_f_name in file_list:
        ccy = os.path.basename(s_f_name).split("_")[2]
        d_f_path = os.path.join(os.path.dirname(s_f_name), "Archiving")
        check_path(d_f_path)
        d_f_name = os.path.join(d_f_path, os.path.basename(s_f_name))
        shutil.move(s_f_name, d_f_name)
        c_f_name = os.path.join(d_f_path, "_".join(os.path.basename(s_f_name).split("_")[2:]))
        with open(d_f_name, "r") as src:
            with open(c_f_name, "w") as dst:
                for line in src:
                    str_list = line.split(",")
                    timestamp = ccy + "_" + str_list[0].replace(".", "") + "_" + str_list[1].replace(":", "")
                    new_strlist = [ccy] + str_list[:-1] + [timestamp]
                    new_line = ",".join(new_strlist)
                    dst.write(new_line + "\n")
        return_file_list.append(c_f_name)
    return return_file_list

#raw_file_parser([r"C:\1.PyProject\historicalDataAnalyzer\rawdata\m1_data\DAT_MT_EURUSD_M1_2016.csv", ])