# get proxies from mogu daili
import requests
import pdb
# generate random user agent
from fake_useragent import UserAgent
import random

PROXY_FETCH_NUM = 1

def generate_proxies():
    proxy_url = "http://piping.mogumiao.com/proxy/api/get_ip_bs?appKey=95cd772cf38e46678d5c11304775f9bd&count=1&expiryDate=0&format=2&newLine=2"
    try:
        raw_proxies = requests.get(proxy_url).text
    except:
        raw_proxies = []
        print("can't get proxies!")
    proxies_list = raw_proxies.split('\r\n')
    proxies_list.pop(-1)
    # print("proxies list is {}".format(proxies_list))
    return proxies_list

def generate_headers():
    try:
        headers = [UserAgent().chrome for i in range(0,PROXY_FETCH_NUM)]
    except:
        print("can't get headers!")
    return headers

def check_proxies(proxy, header):
    check_url = "https://www.zhihu.com/explore"
    html = requests.get(check_url, proxies=proxy, headers=header)
    # pdb.set_trace()
    print("get status code is {}".format(html.status_code))
    print("get html txt is {}".format(html.text))

def proxy_headers(proxies, headers, index):
    # confirm the proxies has same size with headers and index within the range
    if len(proxies) != len(headers) or len(proxies) == 0 or index > len(proxies) - 1:
        print("len of proxies and headers are {} {} with index {}".format(len(proxies), len(headers), index))

        print("proxies not equal to headers or index exceed range")
        exit(-1)
    header = {'User-Agent': "{}".format(headers[index])}
    proxy = {"https" : "https://{}".format(proxies[index])}

    return proxy, header
    
def random_proxy_headers(proxies):
    if len(proxies) == 0:
        print("first generate proxies!")
        exit(-1)
    header = {'User-Agent': "{}".format(UserAgent().random)}
    proxy = {"https" : "https://{}".format(proxies[random.randint(0, len(proxies)-1)])}
    # print("header is {} and proxy is {}".format(header, proxy))
    return proxy, header

if __name__ == "__main__":
    headers = generate_headers()
    print("headers generate success {} with length {}".format(headers[0], len(headers)))
    proxies = generate_proxies()
    proxy, header = proxy_headers(proxies, headers, 0)
    # proxy, header = random_proxy_headers(proxies)
    check_proxies(proxy, header)
    # print("proxies is {}".format(proxies))

