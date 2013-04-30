import unittest
import tables as ta
import numpy as np
import os
import sys

TEST_DATABASE = "test_historical_option_data.h5"




def create_database(file_name):
    h5file = ta.openFile(TEST_DATABASE, 'w')

    # create our class / column descriptor
    class Option(ta.IsDescription):
        underlying_symbol = ta.StringCol(20, pos=0)
        underlying_price = ta.Float32Col(pos=1)
        exchange = ta.StringCol(5, pos=2)
        option_symbol = ta.StringCol(30, pos=3)
        option_ext = ta.StringCol(5, pos=4)
        type = ta.StringCol(10, pos=5)
        Expiration = ta.StringCol(10, pos=6)
        DataDate = ta.StringCol(15, pos=7)
        Strike = ta.StringCol(15, pos=8)
        Last = ta.Float32Col(pos=9)
        Bid = ta.Float32Col(pos=10)
        Ask = ta.Float32Col(pos=11)
        Volume = ta.IntCol(pos=12)
        OpenInterest = ta.IntCol(pos=13)
        IV = ta.Float32Col(pos=14)
        Delta = ta.Float32Col(pos=15)
        Gamma = ta.Float32Col(pos=16)
        Theta = ta.Float32Col(pos=17)
        Vega = ta.Float32Col(pos=18)
        AKA = ta.StringCol(15, pos=19)

    # group = h5file.createGroup('/', 'options', "Option Database")

    # h5file.createTable(group, 'date', Option)
    h5file.createTable('/', 'options', Option)




    print h5file  

    h5file.close()


if __name__ == '__main__':
    create_database(TEST_DATABASE)