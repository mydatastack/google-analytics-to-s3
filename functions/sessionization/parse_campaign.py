import urllib.parse as urlparse
from functools import partial, reduce

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

def parse_url(url: str):
    return urlparse.urlparse(url)

def query_is_empty(url: str):
    return url if len(str(url.query)) != 0 else []

def extract_query_value(xs: list):
    if len(xs) == 0:
        return 'direct=(direct)' 
    else:
        return xs[4]

def split_query(qr: str):
    return dict(item.split('=') for item in qr.split('&'))

def identify_campaign(qr: dict):
    return qr['utm_campaign'] if 'utm_campaign' in qr else '(not set)'

def main(url):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            identify_campaign,
            ]) (url)

if __name__ == '__main__':
    import unittest

    url_utm = 'https://dildoking.de/de/empfehlungen/fuer-sie/top-sextoy-fuer-sie.html?utm_source=newsletter19063_06&utm_medium=email&utm_campaign=newsletter19063'
    url_adwords = 'https://dildoking.de/de/marken-bei-dildoking/top-marken-sextoys/docjohnson-sextoys.html?CAWELAID=120077130000007192&CATRK=SPFID-1&CAAGID=22606803860&CATCI=kwd-49622108&CAPCID=231310162686&CADevice=c&gclid=EAIaIQobChMIzIHd5K7z4wIVDUTTCh0_WgI7EAAYASAAEgKXxvD_BwE'
    url_direct = 'https://dildoking.de/de/eromeo-masturbator-ms-honesty-aircraft-skin.html'

    class TestHandler(unittest.TestCase):

        def test_run_utm(self):
            self.assertEqual(main(url_utm), 'newsletter19063')

        def test_run_adwords(self):
            self.assertEqual(main(url_adwords), '(not set)')

        def test_run_direct(self):
            self.assertEqual(main(url_direct), '(not set)')

    unittest.main()

