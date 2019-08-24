import urllib.parse as urlparse
from functools import partial, reduce

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

channels = ['utm_source', 'gclid', 'gclsrc', 'dclid', 'fbclid', 'mscklid', 'direct']

def match(xs):
    return [s for s in xs if any(xz in s for xz in channel_list)]

def extract_query_value(xs):
    if len(xs) == 0:
        return 'direct=(direct)' 
    else:
        return xs[4]

def parse_url(url):
    return urlparse.urlparse(url)

def query_is_empty(url):
    return url if len(str(url.query)) != 0 else []

def split_query(qr: str):
    return dict(item.split('=') for item in qr.split('&'))

def identify_channel(channel_list: list, qr: dict):
    channel = [s for s in qr if any(xz in s for xz in channel_list)]
    if len(channel) == 0:
        return '(not set)'
    elif channel[0] == 'gclid' or channel[0] == 'gclsrc' or channel[0] == 'dclid':
        return 'google'
    elif channel[0] == 'fbclid':
        return 'facebook'
    elif channel[0] == 'mscklid':
        return 'bing'
    elif channel[0] == 'utm_source':
        return qr[channel[0]]
    elif channel[0] == 'direct':
        return qr[channel[0]] 
    else:
        return '(unknown)' 

def main(url):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            partial(identify_channel, channels),
            ]) (url)

if __name__ == '__main__':
    import unittest

    url_utm = 'https://dildoking.de/de/empfehlungen/fuer-sie/top-sextoy-fuer-sie.html?utm_source=newsletter19063_06&utm_medium=email&utm_campaign=newsletter19063'
    url_adwords = 'https://dildoking.de/de/marken-bei-dildoking/top-marken-sextoys/docjohnson-sextoys.html?CAWELAID=120077130000007192&CATRK=SPFID-1&CAAGID=22606803860&CATCI=kwd-49622108&CAPCID=231310162686&CADevice=c&gclid=EAIaIQobChMIzIHd5K7z4wIVDUTTCh0_WgI7EAAYASAAEgKXxvD_BwE'
    url_direct = 'https://dildoking.de/de/eromeo-masturbator-ms-honesty-aircraft-skin.html'

    class TestHandler(unittest.TestCase):

        def test_run_utm(self):
            self.assertEqual(main(url_utm), 'success')

        def test_run_adwords(self):
            self.assertEqual(main(url_adwords), 'success')

        def test_run_direct(self):
            self.assertEqual(main(url_direct), 'success')

    unittest.main()

