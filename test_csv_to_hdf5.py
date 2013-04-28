import unittest
import tables as ta
import numpy as np
import os
import sys

from csv_to_hdf5 import *

TEST_DATABASE = 'test_historical_option_data.h5'

multiple_string_rows = """A,44.78,*,A130216C00018000,,call,02/16/2013,01/31/2013,18,0,25.95,27.2,0,0,2.8034,0.9676,0.2758,-5.966,0.6791,A130216C00018000
A,44.78,*,A130216P00018000,,put,02/16/2013,01/31/2013,18,0.03,0,0.02,0,10,1.7365,-0.0036,0.0658,-0.5448,0.1003,A130216P00018000
A,44.78,*,A130216C00019000,,call,02/16/2013,01/31/2013,19,0,24.95,26.2,0,0,2.6546,0.9657,0.3054,-5.924,0.7121,A130216C00019000
A,44.78,*,A130216P00019000,,put,02/16/2013,01/31/2013,19,0,0,0.03,0,0,1.7212,-0.0052,0.0932,-0.7586,0.1409,A130216P00019000
A,44.78,*,A130216C00020000,,call,02/16/2013,01/31/2013,20,0,23.95,24.95,0,0,2.0973,0.9801,0.2451,-2.9751,0.4515,A130216C00020000
A,44.78,*,A130216P00020000,,put,02/16/2013,01/31/2013,20,0.04,0,0.03,0,36,1.6266,-0.0056,0.1044,-0.7585,0.1491,A130216P00020000
A,44.78,*,A130216C00021000,,call,02/16/2013,01/31/2013,21,0,22.95,24,0,0,2.0805,0.9748,0.3014,-3.5976,0.5508,A130216C00021000
A,44.78,*,A130216P00021000,,put,02/16/2013,01/31/2013,21,0.04,0,0.03,0,12,1.535,-0.0059,0.1164,-0.7533,0.1569,A130216P00021000"""

string_row = "A,44.78,*,A130216P00018000,,put,02/16/2013,01/31/2013,18,0.03,0,0.02,0,10,1.7365,-0.0036,0.0658,-0.5448,0.1003,A130216P00018000"




class TestImportCSVToH5(unittest.TestCase):


	def setUp(self):
		# Delete any existing files
		if os.path.isfile(TEST_DATABASE):
			os.remove(TEST_DATABASE)
			print "Removing: %s" % (TEST_DATABASE)

		# create database
		self.h5file = ta.openFile(TEST_DATABASE, 'w')

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


		self.h5file.createTable('/', 'options', Option)



	def test_can_import_single_string_row_into_database(self):
		# We start with an entire CSV string of data, in one line.
		split_data = string_row.split(',')
		converted_data = [func(data) for func, data in zip(TYPE_FUNCS,split_data)]

		table = self.h5file.root.options

		table.row["underlying_symbol"] = converted_data[0]
		table.row["underlying_price"] = converted_data[1]
		table.row["exchange"] = converted_data[2]
		table.row["option_symbol"] = converted_data[3]
		table.row["option_ext"] = converted_data[4]
		table.row["type"] = converted_data[5]
		table.row["Expiration"] = converted_data[6]
		table.row["DataDate"] = converted_data[7]
		table.row["Strike"] = converted_data[8]
		table.row["Last"] = converted_data[9]
		table.row["Bid"] = converted_data[10]
		table.row["Ask"] = converted_data[11]
		table.row["Volume"] = converted_data[12]
		table.row["OpenInterest"] = converted_data[13]
		table.row["IV"] = converted_data[14]
		table.row["Delta"] = converted_data[15]
		table.row["Gamma"] = converted_data[16]
		table.row["Theta"] = converted_data[17]
		table.row["Vega"] = converted_data[18]
		table.row["AKA"] = converted_data[19]

		table.row.append()
		table.flush()

		# get the last row
		last_insert = table[-1]
		
		for original, inserted in zip(converted_data, last_insert):

			if original is str:
				self.assertEqual(original, inserted)
			if original is np.float:
				self.assertAlmostEqual(original, inserted)
			if original is np.int:
				self.assertEqual(original, inserted)


	def test_can_import_multiple_string_rows_into_database(self):
		table = self.h5file.root.options

		shape_befoe_insert = table.shape

		for sing_row in multiple_string_rows.split('\n'):

			converted_data = single_row_str_to_converted_data(TYPE_FUNCS, sing_row)

			table.row["underlying_symbol"] = converted_data[0]
			table.row["underlying_price"] = converted_data[1]
			table.row["exchange"] = converted_data[2]
			table.row["option_symbol"] = converted_data[3]
			table.row["option_ext"] = converted_data[4]
			table.row["type"] = converted_data[5]
			table.row["Expiration"] = converted_data[6]
			table.row["DataDate"] = converted_data[7]
			table.row["Strike"] = converted_data[8]
			table.row["Last"] = converted_data[9]
			table.row["Bid"] = converted_data[10]
			table.row["Ask"] = converted_data[11]
			table.row["Volume"] = converted_data[12]
			table.row["OpenInterest"] = converted_data[13]
			table.row["IV"] = converted_data[14]
			table.row["Delta"] = converted_data[15]
			table.row["Gamma"] = converted_data[16]
			table.row["Theta"] = converted_data[17]
			table.row["Vega"] = converted_data[18]
			table.row["AKA"] = converted_data[19]

			table.row.append()
			table.flush()

		# get the last row
		last_insert = table[-1]
		
		for original, inserted in zip(converted_data, last_insert):

			if original is str:
				self.assertEqual(original, inserted)
			if original is np.float:
				self.assertAlmostEqual(original, inserted)
			if original is np.int:
				self.assertEqual(original, inserted)

		# self.assertEqual(table.shape, (8,) )

		# compare table shapes that we inserted the right number of records
		rows_inserted = table.shape[0] - shape_befoe_insert[0]
		rows_in_data = len(multiple_string_rows.split('\n'))

		self.assertEqual(rows_in_data, rows_inserted)


	def test_can_insert_group_of_records(self):
		table = self.h5file.root.options

		shape_befoe_insert = table.shape

		# Let's convert our multiple_string_rows to somethinge useful
		split_rows = multiple_string_rows.split('\n') 
		data = [ single_row_str_to_converted_data(TYPE_FUNCS, row) for row in split_rows]

		table.append(data)
		
		# get the last row
		last_insert = table[-1]
		last_row = data[-1]
		for original, inserted in zip(last_row, last_insert):

			if original is str:
				self.assertEqual(original, inserted)
			if original is np.float:
				self.assertAlmostEqual(original, inserted)
			if original is np.int:
				self.assertEqual(original, inserted)




		# compare table shapes that we inserted the right number of records
		rows_inserted = table.shape[0] - shape_befoe_insert[0]
		rows_in_data = len(multiple_string_rows.split('\n'))

		self.assertEqual(rows_in_data, rows_inserted)


	def tearDown(self):
		self.h5file.close()

		# if raw_input("Delete File y/[n]:") is 'y':
		# 	os.remove(TEST_DATABASE)
		os.remove(TEST_DATABASE)

		print "\nTearing down test database"



if __name__ == "__main__":

	unittest.main()