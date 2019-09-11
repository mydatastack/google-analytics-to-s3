from adapters.s3_trigger import s3_trigger
from adapters.s3_writer import s3_writer, s3_failed
from adapters.sns_reader import sns_reader
from filters.ip import ip_lookup 
from filters.user_agent import ua_lookup 
from filters.general import unpack_ip, unpack_ua, merge_to_dict, add_status, json_decode 
from functools import reduce
from typing import Generator
from utils.logger import log_generator
from utils.init import init, pipe
import json

pipeline = init(
        input_=sns_reader,
        filters=[
                unpack_ip,
                unpack_ua,
                ua_lookup,
                ip_lookup,
                merge_to_dict,
                json_decode,
                add_status,
                ],
        output=s3_writer,
        error=s3_failed
        )

def handler(event: dict, ctx=None) -> ():
    return pipeline(event)


if __name__ == '__main__':
    import unittest

    class TestHandler(unittest.TestCase):
        def test_read_file_data(self):
            pass
        def test_run_main(self):
            with open("./payload_sns.json") as file:
                try:
                    event = json.load(file)
                except Exception as e:
                    print(e)
                else:
                    self.assertEqual(handler(event), "success")

    unittest.main()
