import unittest
from main import handler
import json


class TestHandler(unittest.TestCase):
    def test_read_file_data(self):
        pass
    def test_run_handler(self):
        with open('payload_sns.json') as myfile:
            event = json.load(myfile)
            self.assertEqual(handler(event, None), 'success')

if __name__ == '__main__':
    unittest.main()
