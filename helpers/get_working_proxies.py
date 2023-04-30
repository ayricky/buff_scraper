import asyncio
import time
import warnings

import aiohttp
import aiohttp_socks
import requests
import yaml
from aiohttp_socks import ProxyConnector
from python_socks._errors import ProxyError
from tqdm import tqdm
from tqdm.std import TqdmExperimentalWarning

CONCURRENT_CONNECTIONS = 20

def fetch_proxies(url):
    response = requests.get(url)
    if response.status_code == 200:
        proxies = response.text.split("\n")
        return [proxy.strip() for proxy in proxies if proxy.strip()]
    else:
        print("Failed to fetch proxies")
        return []

class TqdmCounter(tqdm):
    def __init__(self, *args, **kwargs):
        self.good_proxies = 0
        super().__init__(*args, **kwargs)

    def display(self, *args, **kwargs):
        total_str = f"Good proxies: {self.good_proxies}"
        self.set_description_str(total_str, refresh=False)
        super().display(*args, **kwargs)

async def test_proxy(semaphore, proxy, proxy_type, counter):
    async with semaphore:
        test_url = "https://buff.163.com/"
        timeout = aiohttp.ClientTimeout(total=4)

        connector = ProxyConnector.from_url(f"{proxy_type}://{proxy}")

        try:
            start_time = time.time()
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(test_url) as response:
                    if response.status == 200:
                        elapsed_time = time.time() - start_time
                        counter.good_proxies += 1
                        # counter.write(f'Proxy: {proxy} ; Speed: {elapsed_time}')
                        return {'proxy': proxy, 'proxy_type': proxy_type, 'speed': elapsed_time}
        except Exception:
            # counter.write(f"Proxy {proxy} is bad.")
            pass
        finally:
            await connector.close()

def save_working_proxies(proxies, filename):
    with open(filename, "w") as f:
        yaml.dump(proxies, f)

async def main():
    proxy_list_urls = [
        ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "http"),
        ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt", "socks4"),
        ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "socks5"),
    ]
    proxies = []
    for url, proxy_type in proxy_list_urls:
        fetched_proxies = fetch_proxies(url)
        proxies.extend((proxy, proxy_type) for proxy in fetched_proxies)

    semaphore = asyncio.Semaphore(CONCURRENT_CONNECTIONS)
    working_proxies = []

    with TqdmCounter(total=len(proxies), desc="Good proxies: 0") as pbar:
        async def test_proxy_and_update_pbar(semaphore, proxy, proxy_type):
            result = await test_proxy(semaphore, proxy, proxy_type, pbar)
            if result:
                working_proxies.append(result)
            pbar.update(1)

        await asyncio.gather(*(test_proxy_and_update_pbar(semaphore, proxy, proxy_type) for proxy, proxy_type in proxies))

    sorted_proxies = sorted(working_proxies, key=lambda x: x['speed'])
    save_working_proxies(sorted_proxies, "working_proxies.yaml")
    print("Working proxies saved to working_proxies.yaml")

if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)
    asyncio.run(main())
