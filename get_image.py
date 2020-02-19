# -*- coding: utf-8 -*-
"""
Created on Mon Mar 19 11:10:25 2018

"""
import time
import os
import re
import requests
import http.cookiejar as cookielib
from lxml import etree
from aip import AipFace, AipBodyAnalysis
import pandas as pd
import get_proxies
import urllib
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import random
import logging
import pdb
import math
from urllib.request import urlretrieve
# import json

class Log():
    # logging.basicConfig(level = logging.INFO, filename='girl.log', filemode='w', format = '%(asctime)s - %(levelname)s - %(message)s')
    logger = None
    @staticmethod
    def init_logger():
        logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
        Log.logger = logging.getLogger(__name__)


class Proxy():
    proxies = []
    headers = []
    proxy, header = None, None
    session = None
    # proxy usage
    usage = 0

    @staticmethod
    def init_proxies():
        # create session for the program
        requests.adapters.DEFAULT_RETRIES = 2
        Proxy.session = requests.Session()
        Proxy.session.cookies = cookielib.LWPCookieJar(filename='cookie')
        Proxy.session.keep_alive = False
        try:
            Proxy.session.cookies.load(ignore_discard=True)
        except:
            Log.logger.info('Cookie cant load')
        finally:
            pass


    @staticmethod
    def refresh_proxies():
        # generate proxies may fetch empty list so check the len then assign
        try:
            temp_proxies = get_proxies.generate_proxies()
            if len(temp_proxies) > 0:
                proxies = temp_proxies
                headers = get_proxies.generate_headers()
            Log.logger.info("proxy generated! {}".format(proxies))
            Proxy.proxy, Proxy.header = get_proxies.proxy_headers(proxies, headers, 0)
        except:
            Log.logger.info("proxy fetched failed")

    @staticmethod
    def fetch_url(url, infos="url fetch failed"):
        try:
            s = Proxy.session.get(url, proxies=Proxy().proxy, headers=Proxy().header, timeout=5)
        except Exception as e:
            Log.logger.warning(infos + " {}".format(e))
            Proxy.usage = 50
            s = None
    
        Proxy.usage += 1 
        if Proxy.usage > 50:
            Proxy.usage = 0
            Proxy.refresh_proxies()
        return s
def zip_question_url(question_id, offset=0, per_page=20):
    question_url = "https://www.zhihu.com/api/v4/questions/{}/answers?offset={}&limit={}".format(question_id, offset, per_page)
    return question_url
# per page means how many answers one page
def fetch_answer_list(question_id, per_page=20):
    question_page = None
    answer_list = []
    # we should make sure question_url never be None
    while(question_page == None):
        question_url = zip_question_url(question_id, 0, per_page)
        question_page = Proxy.fetch_url(question_url)
        if question_page == None:
            Log.logger.warning("question page get error, will retry")

    answer_count = 0 
    try:
        data = question_page.json()
        # get the answers count
        answer_total = int(data['paging']['totals'])
        # reduce the following page requests
        if answer_total <= per_page:
            for answer in data['data']:
                answer_id = answer['id']
                answer_user = answer['author']['url_token']
                answer_url = "https://www.zhihu.com/question/{}/answer/{}".format(question_id, answer_id)    
                answer_info = {"answer_user": answer_user, "answer_url": answer_url}
                answer_list.append(answer_info)
                answer_count += 1
                
            Log.logger.info("add {} answers to list!".format(answer_count))
            return answer_list

        is_end = bool(data['paging']['is_end'])
    except Exception as err:
        Log.logger.warning("get answer list failed")
        return []

    # get all the page of one question and fetch the answer info
    for i in range(0, int(math.ceil(answer_total / per_page)) * per_page, per_page):
        try:
            question_url = zip_question_url(question_id, i, per_page)
            question_page = Proxy.fetch_url(question_url)
            if question_page == None:
                Log.logger.warning("question page get error, please check the url")
                continue

            data = question_page.json()
            is_end = bool(data['paging']['is_end'])

            for answer in data['data']:
                answer_id = answer['id']
                answer_user = answer['author']['url_token']
                # skip hiden user
                if answer_user == "":
                    Log.logger.info("hider user skip....")
                    continue
                answer_url = "https://www.zhihu.com/question/{}/answer/{}".format(question_id, answer_id)    
                answer_info = {"answer_user": answer_user, "answer_url": answer_url}
                Log.logger.info("add user and url {}".format(answer_info))
                answer_list.append(answer_info)
                answer_count += 1
            if is_end:
                break
        except Exception as e:
            Log.logger.warning("get answer list element failed, loop for others......")
            continue
    Log.logger.info("add {} answers to list!".format(answer_count))
    return answer_list

def fetch_answer_content(answer_url):
    img_urls = []
    content = Proxy.fetch_url(answer_url, infos="per answer content fetched failed ")
    # get html text
    if content  == None:
        return []
    content_text = content.text
    # use BeautifulSoup to parse the content
    bs = BeautifulSoup(content_text, "html.parser")
    # answer content included in class RichText ztext CopyrightRichText-richText
    user_content = bs.find_all("noscript")
    # maybe no image so continue
    if len(user_content) == 0:
        Log.logger.info("this answer has no image")
        return []
    for i in range(0, len(user_content)):
        img_url = str(user_content[i]).split()[-2].split("\"")[1]
        # check img url is valid
        if "http" in img_url:
            img_urls.append(img_url)
    return img_urls


# to say one url should only have one file list to return for face detection
def fetch_user_urls(url):
    answers_api = "https://www.zhihu.com/api/v4/members/{}/answers?limit=20&offset=0".format(url)
    s = Proxy.fetch_url(answers_api, infos="answers json fetched failed")
    if s == None:
        return []
    text = s.json()
    # answer image fetched here whould be better
    try:
        Log.logger.info("get data size {}".format(len(text['data'])))
    except:
        Log.logger.warning("answer list got failed ")
        return []
    img_urls = []
    for data in text['data']:
        question_id = data['question']['id']
        answer_id = data['id']
        answer_url = "https://www.zhihu.com/question/{}/answer/{}".format(question_id, answer_id)    
        img_urls.extend(fetch_answer_content(answer_url))
    if len(img_urls) == 0:
        Log.logger.info("user has no image url {}".format(url))
    return img_urls

def write_image_from_source(filename, fileholder, source):
    try:
        with open(os.path.join(fileholder, filename), "wb") as fd:
            fd.write(source)
    except Exception as e:
        Log.logger.warning("write image failed, please check")

def write_image_from_url(filename, fileholder, url):
    filepath = "{}/{}".format(fileholder, filename) 
    try:
        urlretrieve(url, filepath)
    except Exception as e:
        Log.logger.warning("download image falied")
    
# process the images of the specific user
def process_images(img_urls, author_name, face_detective, fileholder="/mnt/e/questions", following=0, use_face=True):
    girl_num = 0
    # will shuffle the images for not extract image from one answer
    Log.logger.info("first process image {}".format(img_urls[0]))
    # consider the no name user
    if author_name == '':
        author_name += str(int(time.time()) % 5000)

    for image in img_urls:
        if use_face:
            random.shuffle(img_urls)
            s = Proxy.fetch_url(image, infos="per image fetched failed")
            if s == None:
                continue
            person = face_detective(s.content)
            time.sleep(0.3)
            if len(person) > 0:
                filename = "{}_{}_{}.jpg".format(author_name, following, girl_num)
                Log.logger.info("got one girl image {}".format(filename))
                write_image_from_source(filename, fileholder, s.content)
                girl_num += 1

            # if girl_num > 10:
            #    break
        else:
            filename = "{}_{}.jpg".format(author_name, girl_num)
            Log.logger.info("got one girl image {}".format(filename))
            write_image_from_url(filename, fileholder, image)
            girl_num += 1
            if girl_num > 10:
                break
        # make sure the QPS limit of baidu ai 
    return girl_num

def init_aip_method(app_id, api_key, secret_key, method=AipBodyAnalysis):
    client = method(app_id, api_key, secret_key)
    net = AipBodyAnalysis(app_id, api_key, secret_key)
    net.setConnectionTimeoutInMillis(5000)
    net.setSocketTimeoutInMillis(9000) 
    options = {"type": "gender,age"}
    def detective(image):
        try:
            r = client.bodyAttr(image, options)
            if r["person_num"] == 0:
                Log.logger.info("image contain no person")
                return []
            for person in r['person_info']:
                if person['attributes']['gender']['name'] == "男性": 
                    Log.logger.info("male has been detected, discard")
                    return []
                if person['attributes']['gender']['score'] < 0.6: 
                    Log.logger.info("not that much like a girl discard")
                    return []
                if person['attributes']['age']['name'] == "幼儿":
                    Log.logger.info("children has been detected, discard")
                    return []
        except Exception as e:
            Log.logger.warning("image can't be processed {}".format(e))
            return []
        return [r]
    return detective

def create_image_folder(fileholder):
    is_skip = False
    if not os.path.exists(fileholder):
        os.makedirs(fileholder)
    else:
        Log.logger.warning("this question has been created: {}".format(fileholder))
        is_skip = True
    return is_skip

def prepare_users():
    user_file = "zhihu.csv"
    user_frame = pd.read_csv(user_file)
    Log.logger.info("user keys is {}".format(user_frame.keys()))
    # # do some filtering
    # user_following = user_frame['following']
    # user_list = user_frame['self_domain'].str.split(pat='/', expand=True)[2]
    user_frame = user_frame[user_frame['location'].isnull()]
    user_frame = user_frame[user_frame['following'] < 1000]
    user_frame = user_frame[user_frame['answer_num'] < 40]

    user_frame.sort_values(["following","answer_num"], inplace=True)

    user_list = user_frame[['self_domain', 'following']]
    # return user's self domain name
    return user_list
def fetch_images_per_user():
    face_detective = init_aip_method(APP_ID, API_KEY, SECRET_KEY)
    user_list = prepare_users()
    filehoder = "/mnt/e/users/{}".format(int(time.time()))
    create_image_folder(fileholder)
    for i in range(330, user_list.shape[0]):
        Log.logger.info("current url: " + user_list['self_domain'].iloc[i])
        img_urls = fetch_user_urls(user_list['self_domain'].iloc[i])
        if len(img_urls) > 0:
            girl_num = process_images(img_urls, user_list['self_domain'].iloc[i], face_detective, fileholder, user_list['following'].iloc[i])
   
def zip_topic_url(topic_id, offset=0, per_page=10):
    topic_url = "https://www.zhihu.com/api/v4/topics/{}/feeds/essence?offset={}&limit={}".format(topic_id, offset, per_page)
    return topic_url

# per page means how many answers one page
def fetch_question_list(topic_id, per_page=10):
    topic_page = None
    question_list = []
    # we should make sure topic_url never be None
    while(topic_page == None):
        topic_url = zip_topic_url(topic_id, 0, per_page)
        topic_page = Proxy.fetch_url(topic_url)
        if topic_page == None:
            Log.logger.warning("topic page get error, will retry")

    question_count = 0 
    try:
        data = topic_page.json()
        # reduce the following page requests
        for question in data['data']:
            question_id = question['target']['question']['id']
            question_list.append(question_id)
            question_count += 1
            
        Log.logger.info("add {} questions to list!".format(question_count))
        is_end = bool(data['paging']['is_end'])
    
    except Exception as err:
        Log.logger.warning("get question list failed")
        return []

    if is_end:
        return question_list
    # this is the total question id to fetch
    question_total = 10

    # get all the page of one topic and fetch the question info
    for i in range(1, question_total * per_page, per_page):
        try:
            topic_url = zip_topic_url(topic_id, i, per_page)
            topic_page = Proxy.fetch_url(topic_url)
            if topic_page == None:
                Log.logger.warning("topic page get error, please check the url")
                continue

            data = topic_page.json()
            is_end = bool(data['paging']['is_end'])

            for question in data['data']:
                question_id = question['target']['question']['id']
                question_list.append(question_id)
                question_count += 1
            if is_end:
                break
        except Exception as e:
            Log.logger.warning("get question list element failed, loop for others......")
            continue
    Log.logger.info("add {} questions to list!".format(question_count))
    return question_list

def filter_question(answer_list, face_detective, fileholder, test_answer_num=10):
    # select how many answer to test
    is_skip = False
    all_img_count = 0
    female_img_count = 0
    if answer_list.shape[0] > test_answer_num:
        test_answer = answer_list.head(test_answer_num)
        answer_list = answer_list.iloc[test_answer_num : ]
        for i in range(0, test_answer_num):
            img_urls = fetch_answer_content(answer_list['answer_url'].iloc[i])
            if len(img_urls) > 0:
                female_img_count += process_images(img_urls, answer_list['answer_user'].iloc[i], face_detective, fileholder)
            all_img_count += len(img_urls)
        # 50% test image are female, this question not useful for our use
        if female_img_count * 2.5 < all_img_count:
            is_skip = True
    return answer_list, is_skip


def fetch_images_per_question(): 
    face_detective = init_aip_method(APP_ID, API_KEY, SECRET_KEY)
    # topic_list = ["19552223", "19552207", "19584431", "19655944", '19941817', '20034818',
    #   "19561622", "19664390", "19550818", "19561847"]
    topic_list = ["19655944"]
    # topic_list = ["19561622"]
    # topic_list = ["19683311", "19633980", "20077041","19561625"]

    question_list = []

    # you can select topic to get image
    for topic_id in topic_list:
        question_list.extend(fetch_question_list(topic_id))

    # we will loop each question
    for i in range(0, len(question_list)):
        quetion_male_count = 0
        # collect according to specified folder
        fileholder = "/mnt/e/questions/{}".format(question_list[i])
        is_skip = create_image_folder(fileholder)
        if is_skip:
            Log.logger.warning("image folder eixsts, skip this question")
            continue

        Log.logger.info("current quesion: {}".format(question_list[i]))
        answer_list = fetch_answer_list(question_list[i])
        answer_list = pd.DataFrame(answer_list)

        # get 10% of the answers to test
        test_answer_num = int(answer_list.shape[0] / 10) % 200
        # we should make sure it's female or skip this question
        answer_list, is_skip = filter_question(answer_list, face_detective, fileholder, test_answer_num)
        if is_skip:
            continue

        for i in range(0, answer_list.shape[0]):
            img_urls = fetch_answer_content(answer_list['answer_url'].iloc[i])
            if len(img_urls) > 0:
                girl_num = process_images(img_urls, answer_list['answer_user'].iloc[i], None, fileholder, 0, False)


if __name__ == "__main__":

    # baidu clond AI detection project https://console.bce.baidu.com
    APP_ID ="18480308"
    API_KEY = "r1ddkBzIGucxEwCkRArbrZGn"
    SECRET_KEY = "7ZCAHQaVWvuKP2MCKRxbdgiL3eMbGbhy"

    Log.init_logger()
    Proxy.init_proxies()
    Proxy.refresh_proxies()
    Log.logger.info("proxy and header {} {}".format(Proxy.proxy, Proxy.header))
    # get_proxies.check_proxies(Proxy.proxy, Proxy.header)
    # pdb.set_trace()
    # we have 2 method for one is fetch image per user and the other per question
    # fetch_images_per_user()
    fetch_images_per_question()


        # if i > 3:
        #     Log.logger.info("now end")
        #     break
    
