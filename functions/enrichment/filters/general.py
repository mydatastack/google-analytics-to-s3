from typing import Generator
import json

def unpack_ip(xs: Generator[str, None, None]) -> Generator[str, None, None]:
    return (
            (x, x['ip'])
            for x in xs
           )

def unpack_ua(xs: Generator[str, None, None]) -> Generator[str, None, None]:
    return (
            (x, ip, x['user_agent'])
            for x, ip in xs
           )

def merge_to_dict(xs: Generator[tuple, None, None]) -> Generator[dict, None, None]:
    return (
            dict(data, **geo, **ua) 
            for data, geo, ua in xs
           )

def json_decode(xs: Generator[dict, None, None]) -> Generator[dict, None, None]:
    return (
            json.dumps(x) 
            for x in xs
           )

def add_status(xs: Generator[dict, None, None]) -> Generator[dict, None, None]:
    return {
            "result": "Ok", 
            "message": "success",
            "data": "\n".join(list(xs))
          }
