# just test get_proxies
import get_proxies
import time

def test_once():
    proxies = get_proxies.generate_proxies()
    headers = get_proxies.generate_headers()
    proxy, header = get_proxies.proxy_headers(proxies, headers, 0)
    # get_proxies.check_proxies(proxy,header)

def test_fetch_frequency(freq, index=10):
    for i in range(0, index):
        print("test {}".format(i))
        test_once()
        time.sleep(1./freq)

if __name__ == "__main__":
    test_fetch_frequency(0.1)
