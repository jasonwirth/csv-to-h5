import tables as ta
import unittest
import os
import time


TEST_DATABASE = "test_historical_option_data.h5"

from create_h5 import *

class TestCreateDatabase(unittest.TestCase):

    def test_can_open_and_read_newly_created_database(self):
        try:
            os.path.remove(TEST_DATABASE)
            print "Removing old database"
            time.sleep(3)
        except:
            pass


        # Create an empty database
        print "--------- CREATING DATABASE ----------"
        create_database(TEST_DATABASE)
        time.sleep(3)
        print "--------------------------------------"
        # Open our new database and test if it's schema works
        h5file = ta.openFile(TEST_DATABASE)
        print h5file
        h5file.close()


if __name__ == '__main__':
    unittest.main()

