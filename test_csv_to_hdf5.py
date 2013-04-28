import unittest
import tables as ta
import numpy as np

class TestCSVToH5(unittest.TestCase):

	def seteup(self):
		# create database
		self.h5file = ta.openFile('historical_option_data.h5', 'w')


	def teardown(self):
		pass



if __name__ == "__main__":
	unittest.main()