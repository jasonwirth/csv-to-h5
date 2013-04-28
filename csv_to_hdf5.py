import tables as ta
import numpy as np


# TODO: This could be grabbed from a config file. table_config.py
TYPE_FUNCS = [str, \
              np.float, \
              str, \
              str, \
              str, \
              str, \
              str, \
              str, \
              str, \
              np.float, \
              np.float, \
              np.float, \
              np.int, \
              np.int, \
              np.float, \
              np.float, \
              np.float, \
              np.float, \
              np.float, \
              str]


#converted_data = [func(data) for func, data in zip(change_types,single_data)]

def convert_data_types(func_list, data_list):
    return [func(data) for func, data in zip(func_list,data_list)]


def single_row_str_to_converted_data(type_funcs, string):
    split_data = string.split(',')
    return [func(data) for func, data in zip(type_funcs,split_data)]




if __name__ == '__main__':
    main()