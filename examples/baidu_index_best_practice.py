"""
百度指数数据获取最佳实践
此脚本完成
1. 清洗关键词
2. 发更少请求获取更多的数据
3. 请求容错
4. 容错后并保留当前已经请求过的数据，并print已请求过的keywords
"""
from queue import Queue
from typing import Dict, List
import traceback
import time

import pandas as pd
from qdata.baidu_index import get_search_index
from qdata.baidu_index.common import check_keywords_exists, split_keywords

import sys
sys.path.insert(0,r'D:\Chunwu\spider-BaiduIndex\qdata\baidu_index')
import config as cn


cookies = 'CHKFORREG=e9f1adb0cde8e0343211a738917a7342; BAIDUID=B6365B87788E16068BBAFD364045DC54:FG=1; BAIDUID_BFESS=B6365B87788E16068BBAFD364045DC54:FG=1; BIDUPSID=B6365B87788E16068BBAFD364045DC54; PSTM=1698721633; ZFY=OBwbKIogxUr2WLkuFICcZFhMtF3YmKKOQgmVDBBjdkQ:C; BAIDU_WISE_UID=wapp_1709066871752_907; H_PS_PSSID=40010_40170_40210_40206_40224_40294_40291_40287_40285_40317_40319_40079_40365_40352_40375_40367_40359_40409_40415; Hm_lvt_d101ea4d2a5c67dab98251f0b5de24dc=1710367807; BDUSS=lM1cHcyUHpZdTlCaW80dzh5Z2VsWHNNOWtmb3l5dFM3YzJtRmpaWTFCQnZzUmxtSVFBQUFBJCQAAAAAAAAAAAEAAAB9hrE2d2luZHnB1rHysfIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG8k8mVvJPJlM; SIGNIN_UC=70a2711cf1d3d9b1a82d2f87d633bd8a04603803044hteAf5eFaDEztXtUzfUAQsOAZCOZkEqMd12nB2jyTzOV%2BqawjIavvdT0FnDI9WhE0ueyR1XL1KdfsMn6IVOY4f2BQseysGrGO9x8a68IpenSsjC2%2FKCub4zzRtSRh3o2srtL2SW2gosH2jmf8SPX%2FOPHiMHPLkdGM4DsrC1EGSM1bwESaJVKZ9rsWgYFuSMgI4NaCiNiA9Ur1sdCSNApAmzYgp4UtmHHMkdRYLrtLeWdzGCTuKs1X2mPZhaNMhVlGL%2FjJt3HGuG55nuaKSgeA461KcqE9%2FX1eoPBYkgdf8NDUcI6jg5aO8flaG2PTO3m90606776559113137491461885026286; __cas__rn__=460380304; __cas__st__212=5c608d65dc8ed3761f8d5c532f1136182ec05a29bf1833387e9d4326869b031acfaf050bc5746d0ff1b2b3b2; __cas__id__212=53836223; CPTK_212=1807014245; CPID_212=53836223; Hm_lpvt_d101ea4d2a5c67dab98251f0b5de24dc=1710367860; ab_sr=1.0.1_NDY0ZDYzNWU5MDdmNzY5ODFjNTMxZmNkNjU5ZWQ0ZDZmOTdiNjY1YTVlYjdlMjZiYzYxMjg4MTU4OGU5YmE2N2FhOThkYTM1MWQ4MTRkOGJjZTc2MjU1ZTI0ZTRiZjJkNzU5NTY5MWM4YTNmODBjYzJiZTIxODM5Y2EwMWQwNjFlYjRmMmJiMTZjNzA0NWI1MDRjZWQyNTVjM2QyOTRlYQ==; BDUSS_BFESS=lM1cHcyUHpZdTlCaW80dzh5Z2VsWHNNOWtmb3l5dFM3YzJtRmpaWTFCQnZzUmxtSVFBQUFBJCQAAAAAAAAAAAEAAAB9hrE2d2luZHnB1rHysfIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG8k8mVvJPJlM; bdindexid=c7vobd4h0ebjukueea4v04j7r2; RT="z=1&dm=baidu.com&si=9414a640-db60-44c1-a126-561845b8cc03&ss=ltqctw16&sl=4&tt=8tx&bcn=https%3A%2F%2Ffclog.baidu.com%2Flog%2Fweirwood%3Ftype%3Dperf"'


def get_clear_keywords_list(keywords_list: List[List[str]]) -> List[List[str]]:
    q = Queue(-1)

    cur_keywords_list = []
    for keywords in keywords_list:
        cur_keywords_list.extend(keywords)
    
    # 先找到所有未收录的关键词
    for start in range(0, len(cur_keywords_list), 15):
        q.put(cur_keywords_list[start:start+15])
    
    not_exist_keyword_set = set()
    while not q.empty():
        keywords = q.get()
        try:
            check_result = check_keywords_exists(keywords, cookies)
            time.sleep(5)
        except:
            traceback.print_exc()
            q.put(keywords)
            time.sleep(90)

        for keyword in check_result["not_exists_keywords"]:
            not_exist_keyword_set.add(keyword)
    
    # 在原有的keywords_list拎出没有收录的关键词
    new_keywords_list = []
    for keywords in keywords_list:
        not_exists_count = len([None for keyword in keywords if keyword in not_exist_keyword_set])
        if not_exists_count == 0:
            new_keywords_list.append(keywords)

    return new_keywords_list


def save_to_excel(datas: List[Dict],area,kw):
    pd.DataFrame(datas).to_excel(r"D:\Chunwu\spider-BaiduIndex\data\{}_{}_index.xlsx".format(kw,area))


def get_search_index_demo(keywords_list: List[List[str]],area,kw):
    """
        1. 先清洗keywords数据，把没有收录的关键词拎出来
        2. 然后split_keywords关键词正常请求
        3. 数据存入excel
    """
    # 不用清洗，手动测试关键词
    # print("开始清洗关键词")
    requested_keywords = []
    # keywords_list = get_clear_keywords_list(keywords_list)
    # print(keywords_list)
    q = Queue(-1)

    for splited_keywords_list in split_keywords(keywords_list):
        q.put(splited_keywords_list)
    
    print("开始请求百度指数")
    datas = []
    while not q.empty():
        cur_keywords_list = q.get()
        try:
            print(f"开始请求: {cur_keywords_list}")
            for index in get_search_index(
                keywords_list=cur_keywords_list,
                start_date='2011-01-01',
                end_date='2023-01-31',
                cookies=cookies,
                area = area
            ):
                index["keyword"] = ",".join(index["keyword"])
                datas.append(index)
            requested_keywords.extend(cur_keywords_list)
            print(f"请求完成: {cur_keywords_list}")
            time.sleep(10)
        except:
            traceback.print_exc()
            print(f"请求出错, requested_keywords: {requested_keywords}")
            save_to_excel(datas,area,kw)
            q.put(cur_keywords_list)
            time.sleep(180)

    save_to_excel(datas,area,kw)

if __name__ == "__main__":
    keywords_list = [
        ['可兰经'],['妈祖'],['佛教'],['道教'],['伊斯兰教'],['基督教']

    ]
        # ["自杀"],["上吊"],["百草枯"],["跳楼"],["安眠药"],
# ,["窒息"],["烦恼"],["神经衰弱"],["圣经"],["佛经"],["耶稣"]
# ["割腕"],["失业"],["抑郁"],["焦虑"]
#["自杀"],["上吊"],["百草枯"],["跳楼"],["安眠药"],

# ["安乐死"],["失眠"],["压力"],["忧虑"],["自缢"],
        # 
    for k in cn.CITY_CODE:
        get_search_index_demo(keywords_list,cn.CITY_CODE[k],keywords_list[0][0])
        print("{} finished".format(k))

    
