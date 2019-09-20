from typing import Generator
import maxminddb

def extract_ip_data(reader, ua: dict, ip: str) -> dict:
    if ua['device_is_bot']:
        return {'ip': ip}
    else:
        try:
            location = reader.get(ip)
        except Exception as e:
            return {
                    'geo_city': '(not set)',
                    'geo_sub_continent': '(not set)',
                    'geo_postal_code': '(not set)', 
                    'geo_region': '(not set)',
                    'geo_metro': '(not set)',
                    'geo_city_id': '(not set)',
                    'geo_country': '(not set)', 
                    'geo_country_iso': '(not set)', 
                    'geo_continent': '(not set)', 
                    'geo_continent_code': '(not set)',
                    'geo_longitude': '(not set)', 
                    'geo_latitude': '(not set)', 
                    'geo_timezone': '(not set)',
                    'geo_network_domain': '(not set)',
                    'geo_network_location': '(not set)',
                    } 
        else:
            try: 
                return {
                        'geo_continent': location['continent']['names']['en'],
                        'geo_sub_continent': '(not set)',
                        'geo_country': location['country']['names']['en'],
                        'geo_region': location['subdivisions'][0]['names']['en'],
                        'geo_metro': '(not set)',
                        'geo_city': location['city']['names']['en'],
                        'geo_city_id': location['city']['geoname_id'],
                        'geo_network_domain': '(not set)',
                        'geo_network_location': '(not set)',
                        'geo_postal_code': location['postal']['code'],
                        'geo_country_iso': location['country']['iso_code'],
                        'geo_continent_code': location['continent']['code'],
                        'geo_longitude': location['location']['longitude'],
                        'geo_latitude': location['location']['latitude'],
                        'geo_timezone': location['location']['time_zone']
                        } 
            except KeyError as e:
                return {
                        'geo_city': '(not set)',
                        'geo_sub_continent': '(not set)',
                        'geo_postal_code': '(not set)', 
                        'geo_region': '(not set)',
                        'geo_metro': '(not set)',
                        'geo_city_id': '(not set)',
                        'geo_country': '(not set)', 
                        'geo_country_iso': '(not set)', 
                        'geo_continent': '(not set)', 
                        'geo_continent_code': '(not set)',
                        'geo_longitude': '(not set)', 
                        'geo_latitude': '(not set)', 
                        'geo_timezone': '(not set)',
                        'geo_network_domain': '(not set)',
                        'geo_network_location': '(not set)',
                        } 


def ip_lookup(xs: Generator[tuple, None, None]) -> Generator[tuple, None, None]:
    try:
        reader = maxminddb.open_database('./mmdb/GeoLite2-City.mmdb')
    except Exception as e:
        print(e)
        print('something goes wrong, becauset there is no file it throws an error')
        return xs
    else: 
        return (
                (data, extract_ip_data(reader, ua, ip), ua)
                for data, ip, ua in xs
                )
