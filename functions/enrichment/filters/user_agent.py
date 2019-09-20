import user_agents
from typing import Generator

def client_type(ua):
    if ua.is_mobile:
        return 'mobile'
    elif ua.is_tablet:
        return 'tablet'
    elif ua.is_pc:
        return 'desktop'
    else:
        return '(not set)'

def parse_ua(ua: str) -> dict:
    user_agent = user_agents.parse(ua)
    if user_agent.is_bot:
        return {'device_is_bot': True}
    else:
        return {
                'device_is_bot': False,
                'device_client_name': user_agent.browser.family,
                'device_client_version': user_agent.browser.version_string,
                'device_os_name': user_agent.os.family,
                'device_os_version': user_agent.os.version_string,
                'device_device_type': client_type(user_agent),
                'device_is_mobile': user_agent.is_mobile,
                'device_device_name': user_agent.device.family,
                'device_device_brand': user_agent.device.brand,
                'device_device_model': user_agent.device.model,
                'device_device_input': '(not set)',
                'device_device_info': '(not set)', 
                }


def ua_lookup(xs: Generator[str, None, None]) -> Generator[str, None, None]:
    return (
            (x, ip, parse_ua(ua))
            for x, ip, ua in xs
           )
