import unittest
import tables as ta
import numpy as np
import os
import sys

from csv_to_hdf5 import *

TEST_DATABASE = 'test_historical_option_data.h5'
TEST_OPTION_CSV = "options_20130423.csv"


def get_csv_lines(csv_filename):
	with open(csv_filename, 'r') as f:
		csv = f.read()
	csv = csv.strip()
	csv = csv.split('\n')
	return csv

# Read database to get lines
def get_table_shape(h5_filename):
	h5file = ta.openFile(h5_filename, 'r')
	table = h5file.root.options
	table_size = table.shape
	h5file.close()
	return table_size


class TestLoadRealCSV(unittest.TestCase):

	def test_load_real_csv_with_bulk_insert(self):


		lines_in_csv = len(get_csv_lines(TEST_OPTION_CSV))
		print "lines in csv:", lines_in_csv

		shape_before = get_table_shape(TEST_DATABASE)
		print "shape_before:", shape_before[0]
		
		print "Inserting records...."
		insert_bulk_data_in_table(TEST_DATABASE, TEST_OPTION_CSV)

		shape_after = get_table_shape(TEST_DATABASE)
		print "shape_after:", shape_after[0]

		rows_inserted = shape_after[0] - shape_before[0]
		print "rows inserted", rows_inserted
		
		self.assertEqual(lines_in_csv, rows_inserted)




if __name__ == "__main__":

	unittest.main()