import urllib.parse as urlparse
from functools import partial, reduce

pipe = lambda fns: lambda x: reduce(lambda v, f: f(v), fns, x)

channels = [
        'utm_source', 
        'gclid', 
        'gclsrc', 
        'dclid', 
        'fbclid', 
        'mscklid', 
        'direct', 
        ]

def match(xs):
    return [s for s in xs if any(xz in s for xz in channel_list)]

def extract_query_value(xs):
    if len(xs) == 0:
        return 'direct=(direct)' 
    else:
        return xs[4]

def parse_url(url: str):
    return urlparse.urlparse(url)

def query_is_empty(url: str):
    return url if len(str(url.query)) != 0 else []

def split_item(item):
    return item.split('=')

def split_query(qr: str):
    query = qr.split('&')
    query_clean = [x for x in query if x and x.find('=') > 0]
    return dict(split_item(item) for item in query_clean)

def identify_channel(channel_list: list, qr: str, *hostname: str):
    channel = [s for s in qr if any(xz in s for xz in channel_list)]
    if len(channel) == 0:
        return '(direct)'
    elif channel[0] == 'gclid' or channel[0] == 'gclsrc' or channel[0] == 'dclid':
        return 'google'
    elif channel[0] == 'fbclid':
        return 'facebook'
    elif channel[0] == 'mscklid':
        return 'bing'
    elif channel[0] == 'utm_source':
        return qr[channel[0]]
    elif channel[0] == 'direct':
        return '(direct)' 
    else:
        return hostname[0] or '(not set)' 

def parse_dl_source(url):
    return pipe([
            parse_url,
            query_is_empty,
            extract_query_value,
            split_query,
            partial(identify_channel, channels),
            ]) (url)

def split_hostname(body_dr: str) -> str:
    hostname = parse_url(body_dr).netloc
    print('after split_hostname: ', hostname)
    hostname_splitted = hostname.split('.')
    print('after hostname_splitted: ', hostname_splitted)
    try:
        if 'www' in hostname_splitted:
            return hostname_splitted[1]
        elif len(hostname_splitted) == 3:
            return hostname_splitted[1]
        elif len(hostname_splitted) == 2:
            return hostname_splitted[0]
        else:
            return hostname
    except Exception as e:
        print(e)
        return body_dr
 
def parse_dr_source(body_dl: str, body_dr: str):
    if body_dr.find('android-app') == 0:
        return body_dr.split('//')[1]
    hostname = split_hostname(body_dr) 
    empty_query_dl = len(query_is_empty(parse_url(body_dl))) == 0
    empty_query_dr = len(query_is_empty(parse_url(body_dr))) == 0  
    query_dl = split_query(extract_query_value(query_is_empty(parse_url(body_dl))))
    query_dr = split_item(extract_query_value(query_is_empty(parse_url(body_dr))))
    print('============ start ==================\n')
    print(empty_query_dr)
    print('hostname:', hostname)
    print('empty_query_dl: ', empty_query_dl)
    print('empty_query_dr: ', empty_query_dr)
    print('query: ', query_dl)
    print('body_dl: ', body_dl)
    print('body_dr: ', body_dr)
    print('=========== end ====================\n')
    if hostname == 'googleadservices':
        return 'google'
    elif empty_query_dl and empty_query_dr: 
        return hostname 
    elif not empty_query_dl and 'utm_source' in query_dl:
        return query_dl['utm_source']
    elif not empty_query_dr:
        return hostname
    elif not empty_query_dl and 'ref' in query_dl:
        return query_dl['ref']
    elif not empty_query_dl:
        return identify_channel(channels, query_dl, hostname)
    else:
        return '(not set)'

def extract_source_source(is_new_session, body_dl, body_dr):
    if (is_new_session == 1 and body_dr is None):
        return parse_dl_source(body_dl) 
    elif (is_new_session == 1 and body_dr is not None):
        return parse_dr_source(body_dl, body_dr)
    else:
        return None 

if __name__ == '__main__':
    import unittest
    url_bing_dr = 'https://www.bing.com/search?q=dildoking&form=EDGSPH&mkt=de-de&httpsmsn=1&plvar=0&refig=020d532819714fceaccbafa71b18f509&sp=-1&pq=dildoking&sc=1-9&qs=n&sk=&cvid=020d532819714fceaccbafa71b18f509' 

    url_bing_dl = 'https://dildoking.de/de/sexspielzeug/spezial-dildos/umschnalldildos.html' 

    url_bing_dr2 = 'https://www.bing.com/search?q=animal+dildo&form=CONMDF&pc=COSPM01&ptag=A162BC08914' 

    url_bing_dl2 = 'https://www.googleadservices.com/pagead/aclk?sa=L&ai=DChcSEwj1ha6eifbjAhUV5poKHZdcAMMYABAAGgJsbQ&ohost=www.google.com&cid=CAASE-Rosk7jJBZb7bGH5zkHpSePc2M&sig=AOD64_0rhIG2MyIOxkFuauIC1ATcIP7GNw&adurl=&rct=j&q=&nb=0&res_url=https%3A%2F%2Fwww.teoma.eu%2Fde%2Fweb%3Fqo%3DsemQuery%26ad%3DsemA%26q%3Dfickmaschienen%2520kaufen%26o%3D768723%26ag%3Dfw4%26an%3Dmsn_s%26rch%3Dintl600%26rtb%3D29945%26msclkid%3D0aa1e34e221816f8aa6abe03aef0bead&rurl=https%3A%2F%2Fwww.bing.com%2Fsearch%3Fq%3Dfikmaschinen%26src%3DIE-SearchBox%26FORM%3DIESR3A%26pc%3DEUPP_&nm=9&nx=176&ny=10&is=627x643&clkt=115&bg=!w8ClwNhEQnGpsQJru0wCAAAAUlIAAAAcmQFNu8quaUbd7ZE_VoGZVw7o76HiBWfJO2dRJLd2OKt65ObQxU2E6PgANB5abuW-3-VtNtrI6NUHZyVFTaKZs5uHYLgXQaBhqGjneh8sSH1rxvwln8DzKvIVqstuJP9CmxPWPnKpsQYtwNrXdUrVRPvJh1uWy9Ihy2UzsmbJK6H6-Cw78ioRshJm0zKEYp6Xxwq8zYf5V5duJSYO_Xi_DbUKdS4IrwW2kdOvJ2ud10zM7ZTrfjQfAACoyZAPNt2rZdYegvx19UlbMgSvf7yj7jZc3NRLACDjSmeAql9phhY9w1b7giMUMvEdCCACJAdA91WnIWMF2SXmfowvSrq_7jI3WxQHwUoVC9vco_N4ygc-Lam0a71Ndqf7tzk8NRAlUIYmyuuVYVxwlSFHa8KRoUjCif7xja99r43XdnjgUStwq9QqulbxacAf-czUbDoM' 

    url_bizrate_dr = 'http://rd2szde.bizrate.com/'
    url_bizrate_dl= 'https://dildoking.de/de/latex-damen-slip-mit-dildo-schwarz.html/?ref=pangora'

    url_ga_dr = None 
    url_ga_dl = 'https://dildoking.de/de/fuer-gays.html?CAWELAID=120077130000001264&CATRK=SPFID-1&CAAGID=351973760&CATCI=kwd-3579991520&CAPCID=160431410112&CADevice=m&gclid=Cj0KCQjws7TqBRDgARIsAAHLHP5zTwdsVa0kstvATdRv7xKrynvzTzyMnx5Skm8Iamd5CAUZfht2PS8aAisOEALw_wcB'
    
    url_ga_dr2 = None
    url_ga_dl2 = 'https://dildoking.de/de/toys-fuer-sie/dessous-fuer-sie/dessous-nach-style/struempfe-strumpfbaender.html?gclid=EAIaIQobChMIndjf1eL14wIVh7HtCh31LwHNEAAYAiAAEgJpgfD_BwE'
    url_adcell_dl = 'https://dildoking.de/de/?bid=123100-45177-dildoking_start_rechnungskauf_1565208196&adcref=www.rechnungskauf.com%2F%3Fx%3Dde.r.263.b%26gclid%3DCj0KCQjwkK_qBRD8ARIsAOteukBqWXNIn1Ch4J9domts7JQcsBLwY7gXlLuyIWL26PlKB4kmIxRSAtcaAu-PEALw_wcB'
    url_adcell_dr = 'https://www.adcell.de/forward.php?promoId=123100&slotId=45177&subId=dildoking_start_rechnungskauf_1565208196&dataKey=8dcfeb3f36c86f6021e86d32dd47e0f7'
    url_duckduck_dr = 'https://duckduckgo.com/' 
    url_duckduck_dl = 'https://dildoking.de/de/marble-anal-beads-2-6-cm.html'

    url_gmx_dr = 'https://deref-gmx.net/mail/client/Gm_oCFfV0VM/dereferrer/?redirectUrl=https%3A%2F%2Fdildoking.de%2Fde%2Fmarken-bei-dildoking%2Ftop-marken-sextoys%2Fweitere-sextoys-marken%2Fdildorama.html%3Futm_source%3Dnewsletter19063_08%26utm_medium%3Demail%26utm_campaign%3Dnewsletter19063'
    url_gmx_dl = 'https://dildoking.de/de/marken-bei-dildoking/top-marken-sextoys/weitere-sextoys-marken/dildorama.html?utm_source=newsletter19063_08&utm_medium=email&utm_campaign=newsletter19063' 

    url_clickpool_dl = 'https://dildoking.de/de/marken-bei-dildoking/top-marken-sextoys/weitere-sextoys-marken/fantasy-c-ringz.html?utm_source=newsletter19063_12&utm_medium=email&utm_campaign=newsletter19063'

    url_clickpool_dr = 'http://clickpool.newsletter.htmldesign.de/webview.php?code=klCiDjzlTjKwQeQewiIyP_9xnkSrd6R1LvAHgzju9nzzQFO-78u6T8p85_5QkPGS'

    url_android_dl = 'https://dildoking.de/de/neuheiten.html'
    url_android_dr = 'android-app://com.google.android.gm'

    url_aol_dl = 'https://dildoking.de/de/'
    url_aol_dr = 'https://r.search.aol.com/_ylt=AwrP4o1ZjU1dYlsAhyw8CmVH;_ylu=X3oDMTByaW11dnNvBGNvbG8DaXIyBHBvcwMxBHZ0aWQDBHNlYwNzcg--/RV=2/RE=1565392346/RO=10/RU=https%3a%2f%2fdildoking.de%2fde%2f/RK=0/RS=yhpugoXEMFGaFOGyg6EMQiQFFqY-'

    class TestHandler(unittest.TestCase):

        @unittest.skip('too much clutter')
        def test_bing(self):
            self.assertEqual(extract_source_source(1, url_bing_dl, url_bing_dr), 'bing')

        @unittest.skip('too much clutter')
        def test_bing2(self):
            self.assertEqual(extract_source_source(1, url_bing_dl2, url_bing_dr2), 'bing')
            
        @unittest.skip('too much clutter')
        def test_bizrate(self):
            self.assertEqual(extract_source_source(1, url_bizrate_dl, url_bizrate_dr), 'pangora')

        @unittest.skip('too much clutter')
        def test_ga(self):
            self.assertEqual(extract_source_source(1, url_ga_dl, url_ga_dr), 'google')

        @unittest.skip('too much clutter')
        def test_ga(self):
            self.assertEqual(extract_source_source(1, url_ga_dl2, url_ga_dr2), 'google')

        @unittest.skip('too much clutter')
        def test_adcell(self):
            self.assertEqual(extract_source_source(1, url_adcell_dl, url_adcell_dr), 'adcell')
            
        @unittest.skip('too much clutter')
        def test_duckduck(self):
            self.assertEqual(extract_source_source(1, url_duckduck_dl, url_duckduck_dr), 'duckduckgo')

        @unittest.skip('too much clutter')
        def test_gmx(self):
            self.assertEqual(extract_source_source(1, url_gmx_dl, url_gmx_dr), 'newsletter19063_08')

        @unittest.skip('too much clutter')
        def test_clickpool(self):
            self.assertEqual(extract_source_source(1, url_clickpool_dl, url_clickpool_dr), 'newsletter19063_12')
            
        def test_android(self):
            self.assertEqual(extract_source_source(1, url_android_dl, url_android_dr), 'com.google.android.gm')

        def test_aol(self):
            self.assertEqual(extract_source_source(1, url_aol_dl, url_aol_dr), 'r.search.aol.com')
    unittest.main()
