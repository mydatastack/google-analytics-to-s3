from adapters.stdin import s3_trigger 
from adapters.stdout import main as outmain
from adapters.stderr import main as invalidmain
from filters.ip import main as ipmain
from filters.user_agent import main as uamain
from functools import reduce
import json

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

def init(stdin, filters, stdout, stderr): 
    def inner(event: dict):
        return pipe([
                    stdin,
                    *filters,
                    stdout,
                    stderr
                    ]) (event)
    return inner

pipeline = init(
        stdin=s3_trigger,
        filters=[ipmain, uamain],
        stdout=outmain,
        stderr=invalidmain
        )

def main(event: dict, ctx=None) -> ():
    pipeline(event)





if __name__ == '__main__':
    import unittest

    class TestHandler(unittest.TestCase):
        def test_read_file_data(self):
            pass
        def test_run_main(self):
            with open("./payload_s3.json") as file:
                try:
                    event = json.load(file)
                except Exception as e:
                    print(e)
                else:
                    self.assertEqual(main(event), "success")

    unittest.main()
