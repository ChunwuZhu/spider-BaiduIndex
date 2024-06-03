"""
百度指数数据获取最佳实践
此脚本完成
1. 清洗关键词
2. 发更少请求获取更多的数据
3. 请求容错
4. 容错后并保留当前已经请求过的数据，并print已请求过的keywords
"""
import sys
sys.path.insert(0,r'D:\code\spider-BaiduIndex\qdata\baidu_index')
from queue import Queue
from typing import Dict, List
import traceback
import time

import pandas as pd
from qdata.baidu_index import get_search_index
from qdata.baidu_index.common import check_keywords_exists, split_keywords


import config as cn


cookies = 'CHKFORREG=0973c647d6dea2c09b45fcae98ea5c13; BIDUPSID=C24A8733C285934963452E7E15F168B2; PSTM=1582612988; BAIDUID=6E49AE070E9B9A48074C4110B4381006:FG=1; BAIDUID_BFESS=6E49AE070E9B9A48074C4110B4381006:FG=1; ZFY=eoXpnYifPIXCFINOnWgmCK9ry4xMEqDJ5m2Fr6uxpZA:C; BDRCVFR[OEHfjv-pq1f]=mk3SLVN4HKm; H_PS_PSSID=; BA_HECTOR=ah0h2k85018g84850l01a0g51hvqs591k; PSINO=7; delPer=0; BDORZ=FFFB88E999055A3F8A630C64834BD6D0; BCLID=11597710498862607301; BCLID_BFESS=11597710498862607301; BDSFRCVID=lGCOJexroG0wAwcjpq9U77P72opWxY5TDYrELPfiaimDVu-Vc4HAEG0Pts1-dEu-S2EwogKK3gOTH6KF_2uxOjjg8UtVJeC6EG0Ptf8g0M5; BDSFRCVID_BFESS=lGCOJexroG0wAwcjpq9U77P72opWxY5TDYrELPfiaimDVu-Vc4HAEG0Pts1-dEu-S2EwogKK3gOTH6KF_2uxOjjg8UtVJeC6EG0Ptf8g0M5; H_BDCLCKID_SF=tRAOoC8-fIvEDRbN2KTD-tFO5eT22-usagvm2hcHMPoosIJ6LJJjMR8LeJ_fth8ftDOj--nwJxbUotoHXh3tMt_thtOp-CrpKbn75l5TtUJMqIDzbMohqqJXQqJyKMnitKv9-pPKWhQrh459XP68bTkA5bjZKxtq3mkjbPbDfn028DKu-n5jHj3yDate3H; H_BDCLCKID_SF_BFESS=tRAOoC8-fIvEDRbN2KTD-tFO5eT22-usagvm2hcHMPoosIJ6LJJjMR8LeJ_fth8ftDOj--nwJxbUotoHXh3tMt_thtOp-CrpKbn75l5TtUJMqIDzbMohqqJXQqJyKMnitKv9-pPKWhQrh459XP68bTkA5bjZKxtq3mkjbPbDfn028DKu-n5jHj3yDate3H; Hm_lvt_d101ea4d2a5c67dab98251f0b5de24dc=1677553893; BDUSS=IxMzAxdjBnU2tlMmVaekhwd0JITlVNOGIxb3lGMTlLZEtDOUczeUxLUld-aVJrSVFBQUFBJCQAAAAAAAAAAAEAAAAOU-831rjP8sewt73YvLfnAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFZx~WNWcf1jTj; SIGNIN_UC=70a2711cf1d3d9b1a82d2f87d633bd8a04275664566McVwtIh9VN0NcQkq+B3kvOPWXynEXnKeNP1KZ9YtZxpwyytwex7UWb9vcTWiGGN/Moj7VzAG2qcjCsIvSLw0mkBN49oeG1/Hxefc6/w29bNtXHp7c8siYdJ4rj/7RefhRm4wKI0lC5C5xBOyQ3jbadKyJUKwabgX5Ai/Gpc2n/yYij3Fww5ZIUNVivl24EPyRm4wKI0lC5C5xBOyQ3jbadKyJUKwabgX5Ai/Gpc2n/wzR+3KpD7vsC2hUFucEP4qbe5gdZfgkvDHqvbrQvS/jVFBoUNQdFnvfmrW/xR7PhqTSLypjyqmk78IuYyF6m/wOMkIqgD5Gf239AtSsYj09qd7RMEkTYs414EXz0XlDCQ=38303721965552879514881150441129; __cas__rn__=427566456; __cas__st__212=04d2cc74be755c07e37c2edba0398033bf1efb7e1605c5c11014a9059a898d1db1b2b8b81bb158c6751b8386; __cas__id__212=45734973; CPID_212=45734973; CPTK_212=1968410004; Hm_up_d101ea4d2a5c67dab98251f0b5de24dc={"uid_":{"value":"938431246","scope":1}}; ab_sr=1.0.1_NDhhZjA1YzlkOWI1YzMyNzYyZTNjZmI3OTYxZmI5OGIxMGZhYzBjZjIwYzNlM2RjMmUxZDc2YjgyMzMzMGZlOGE4ODgzNWEyZGEwMDU2YTVkMTgxM2JiY2EyY2I0YWYzYmE4YWE0MzllNTE5OGMxNThkYjZkYzMwNWQ0NGM2Njg0MzljNjgwZGQ5ZTVjYjgxZjcwMWQ0MmYxNmNjNDU5OQ==; bdindexid=u294mvm25va2c045cl2faigum7; RT="z=1&dm=baidu.com&si=e42263f2-114a-45ea-8600-8e73189db03c&ss=lenob1rt&sl=5&tt=7jc&bcn=https://fclog.baidu.com/log/weirwood?type=perf"; Hm_lpvt_d101ea4d2a5c67dab98251f0b5de24dc=1677554028'


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
    pd.DataFrame(datas).to_excel(r"D:\code\spider-BaiduIndex\data\{}_{}_index.xlsx".format(kw,area))


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
            time.sleep(5)
        except:
            traceback.print_exc()
            print(f"请求出错, requested_keywords: {requested_keywords}")
            save_to_excel(datas,area,kw)
            q.put(cur_keywords_list)
            time.sleep(180)

    save_to_excel(datas,area,kw)

if __name__ == "__main__":
    keywords_list = [
        ["窒息"],["烦恼"],["神经衰弱"],["圣经"],["佛经"],

    ]
# ["安乐死"],["失眠"],["压力"],["忧虑"],["自缢"],

# ,["耶稣"]

#["自杀"],["上吊"],["百草枯"],["跳楼"],["安眠药"],

        # ["割腕"],["失业"],["抑郁"],["焦虑"]
    for k in cn.CITY_CODE1:
        get_search_index_demo(keywords_list,cn.CITY_CODE1[k],keywords_list[0][0])
        print("{} finished".format(k))

    
