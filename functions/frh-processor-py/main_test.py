import unittest
import json
from main import handler


class TestHandler(unittest.TestCase):
    def test_read_file_data(self):
        pass
    def test_run_handler(self):
        with open('payload.json') as file:
            try:
                event = json.load(file)
            except:
                print('payload.json can\'t be parsed')
            else:
                self.assertEqual(len(handler(event, None).get('records')), 11)


if __name__ == '__main__':
    unittest.main()
