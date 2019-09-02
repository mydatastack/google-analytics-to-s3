import urllib.parse as urlparse
from functools import partial, reduce

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

def parse_url(url: str) -> tuple:
    return urlparse.urlparse(url)

def path_is_empty(url: str) -> str:
    return url.path if len(str(url.path)) != 0 else None 

def extract_path_value(path: str) -> str:
    if path == None:
        return '' 
    else:
        return path 

def split_path(path: str) -> list:
    return path.split('/')[1:]

def construct_levels(p: list):
    path = list(filter(None, p))
    if len(path) == 1:
        return [str('/' + path[0]), '', '', ''] 
    if len(path) == 2:
        return [str('/' + path[0]), str('/' + path[1]), '', ''] 
    if len(path) == 3:
        return [str('/' + path[0]), str('/' + path[1]), str('/' + path[2]), '']
    if len(path) >= 4:
        return [str('/' + path[0]), str('/' + path[1]), str('/' + path[2]), str('/' + path[3])]
    else:
        return ['', '', '', '']
    
def main(url):
    return pipe([
            parse_url,
            path_is_empty,
            extract_path_value,
            split_path,
            construct_levels,
            ]) (url)

if __name__ == '__main__':
    import unittest
    # having fun with unit tests! ;-)
    url_level_one = 'https://dildoking.de/de/'
    url_level_two = 'https://dildoking.de/de/7-heaven-leggings-valera-wetlook-black.html' 
    url_level_three = 'https://dildoking.de/de/gays-for-life/hello-world.html'
    url_level_four = 'https://dildoking.de/de/gays-for-life/dildoking/penis-black.html'
    url_hostname_slash = 'https://dildoking.de/'
    url_android = 'android-app://com.google.android.gm'
    url_hostname = 'https://dildoking.de'
    url_not_parsed = '/de/sexspielzeug/analtoys/plugs/xxl-plugs.html'

    class TestHandler(unittest.TestCase):

        def test_page_path_level_one(self):
            self.assertEqual(main(url_level_one), ['/de', '', '', ''])

        def test_page_path_level_two(self):
            self.assertEqual(main(url_level_two), 
                    ['/de', '/7-heaven-leggings-valera-wetlook-black.html', '', ''])

        def test_page_path_level_three(self):
            self.assertEqual(main(url_level_three), 
                    ['/de', '/gays-for-life', '/hello-world.html', ''])

        def test_page_path_level_four(self):
            self.assertEqual(main(url_level_four), 
                    ['/de', '/gays-for-life', '/dildoking', '/penis-black.html'])

        def test_page_path_hostname_slash(self):
            self.assertEqual(main(url_hostname_slash), ['', '', '', ''])

        def test_page_path_hostname(self):
            self.assertEqual(main(url_hostname), ['', '', '', ''])

        def test_page_path_android(self):
            self.assertEqual(main(url_android), ['', '', '', ''])

        def test_page_path_not_parsed(self):
            self.assertEqual(main(url_not_parsed), ['/de', '/sexspielzeug', '/analtoys', '/plugs'])

    unittest.main()
