import tables as ta
import numpy as np
import sys
import os
from docopt import docopt
import re
doc_string = """
Usage: csv_to_hdf5.py [-i <CSVFILE>] [-o <H5FILE>]
       csv_to_hdf5.py [-d <DIRECTORY>] [-o <H5FILE>]
       csv_to_hdf5.py [-d <DIRECTORY>] [-p <PATTERN>] [-o <H5FILE>]

Options:
  -i <CSVFILE>    file to import
  -o <H5FILE>     h5 database to use
  -d <DIRECTORY>  Import all csv files in the directory
  -p <PATTERN>    Searches file names for a regex pattern. E.g. r"(options_)"
"""


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


def convert_data_types(func_list, data_list):
    return [func(data) for func, data in zip(func_list,data_list)]


def single_row_str_to_converted_data(type_funcs, string):
    split_data = string.split(',')
    return [func(data) for func, data in zip(type_funcs,split_data)]


def read_csv(file_name, type_functions):
    with open(file_name, 'r') as f:
        file_contents = f.read()

    file_contents = file_contents.strip()
    lines = file_contents.split('\n')

    # implicit header check
    if "underlying" in lines[0].lower():
        lines = lines[1:]


    cleaned_data = []
    for line in lines:
        split_line = line.split(',')
        zip(type_functions, split_line)
        converted_data = single_row_str_to_converted_data(type_functions, line)
        cleaned_data.append(converted_data)

    return cleaned_data 


def insert_bulk_data_in_table(h5_filename, csv_filename):
    
    # read the csv file annd parse the data
    data = read_csv(csv_filename, TYPE_FUNCS)

    # open our h5 file
    hf5file = ta.openFile(h5_filename, 'a')
    table = hf5file.root.options

    # insert csv data into the h5 table
    # for converted_data in data:
    #     table.row["underlying_symbol"] = converted_data[0]
    #     table.row["underlying_price"] = converted_data[1]
    #     table.row["exchange"] = converted_data[2]
    #     table.row["option_symbol"] = converted_data[3]
    #     table.row["option_ext"] = converted_data[4]
    #     table.row["type"] = converted_data[5]
    #     table.row["Expiration"] = converted_data[6]
    #     table.row["DataDate"] = converted_data[7]
    #     table.row["Strike"] = converted_data[8]
    #     table.row["Last"] = converted_data[9]
    #     table.row["Bid"] = converted_data[10]
    #     table.row["Ask"] = converted_data[11]
    #     table.row["Volume"] = converted_data[12]
    #     table.row["OpenInterest"] = converted_data[13]
    #     table.row["IV"] = converted_data[14]
    #     table.row["Delta"] = converted_data[15]
    #     table.row["Gamma"] = converted_data[16]
    #     table.row["Theta"] = converted_data[17]
    #     table.row["Vega"] = converted_data[18]
    #     table.row["AKA"] = converted_data[19]

    #     table.row.append()
    #     table.flush()

    table.append(data)

    hf5file.close()

        


if __name__ == '__main__':
    args = docopt(doc_string)

    # If the user uses '~' for the home directory, get the full path
    if args['-d'].startswith("~"):
        args['-d'] = os.path.expanduser(args['-d'])

    # Use the abs path for our directory
    if args['-d']:
        args['-d'] = os.path.abspath(args['-d'])


    print args

    #====================================================================
    # Usage: 
    #        csv_to_hdf5.py [-i <CSVFILE>] [-o <H5FILE>]
    #====================================================================
    # Input a single file
    if args['-i']: 
        csv_filename = args['-i']
        h5_filename = args['-o']
        print "Inserting records..."
        insert_bulk_data_in_table(h5_filename, csv_filename)

    #====================================================================
    #  Usaege:
    #        csv_to_hdf5.py [-d <DIRECTORY>] [-o <H5FILE>]
    #        csv_to_hdf5.py [-d <DIRECTORY>] [-p <PATTERN>] [-o <H5FILE>]
    #====================================================================
    if args['-d']:
        # Get files in the directory
        dir_list = os.listdir(args['-d'])

        # filter directory for only csv files
        dir_list = [filename for filename in dir_list if filename.endswith(".csv")]

        # filter the list for patterns
        # [-p <PATTERN>]
        if args['-p']:
            dir_list = [filename for filename in dir_list if re.search(args['-p'], filename)]
            
        dir_list = [os.path.join(args['-d'], filename) for filename in dir_list]

        for filename in dir_list:
            print filename
        print "Do you want to import %d files?" % (len(dir_list))
        process_files = raw_input('y/[n]  ')
        if process_files is "y":
            for i, filename in enumerate(dir_list):
                print "-" * 79
                print "processing: %d: %s" % (i, filename)
                insert_bulk_data_in_table(args['-o'], filename)





