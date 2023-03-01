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
sys.path.insert(0,r'D:\code\spider-BaiduIndex\qdata\baidu_index')
import config as cn


cookies = 'CHKFORREG=277b6d93af449ec79f6b42c369420054; BIDUPSID=A8F7665EE559600A6D8578FBEDCBF2EB; PSTM=1666403848; BDUSS=XU1QzJja3pGV0pNdHBGQUlrRUxEdnBHT3JOcWQyRlJ-Z2ZMczBDcldncWhaYVJqRVFBQUFBJCQAAAAAAAAAAAEAAABSc1qRbWFwZG9ntcQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKHYfGOh2Hxje; ZFY=8BNsa5fIS:BPoZsyKvdV3d72c6uoSqd404Xb7n65ExXA:C; __bid_n=1868dd613c69dc12a44207; FPTOKEN=6SSh2SE4sOJpkzsKPqaHuP3/EA5eSDeOVJpb5GIQgPgCKDB+nHf2eP4qIYh3gglNHFAzwG69/3tlEV26r4ZlysLM7hKmBf9DIgE4GlDfOmK11p+o/e3cSqJgQGBAaUc1+m4gR9Bq2qRfKRJkm8w5k703fkcQWGxvUuCfqvnKp43ioQLxbU7/xBZVCKJuJjm5VjAnf2Ry9uiQxYiUs3pq7NyA5j1Q+8dF6FejqipHgCM+QCjA7sXLyj4ZuRI5XtlyIQ98rnViCT9E9VTsoTX/dqcXF6kR2kLzmKm1AdXfyPKuk4xD6DbkO5Mw8Q+2jjET1BBlbzZz7S/Z9yryH5kSzbZShmpNSJErNd3WFz7/ae/9AoKFH62lM/ATCjqVSmUCEv1EGT/ypD/FtyKo+Y7HkQ==|p6UpftcWU48yaYeEf8bD25RLfPrjujBNirZr0uPOXc4=|10|a62f22b554c01d448fcdbad3ff71b1a1; BAIDUID=48AB4C038C744D133170786659BB70C3:FG=1; BAIDUID_BFESS=48AB4C038C744D133170786659BB70C3:FG=1; BA_HECTOR=0l2k2l0l21842g0h2k0h8giu1hvpc6h1k; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; MCITY=-340:; BDRCVFR[S4-dAuiWMmn]=I67x6TjHwwYf0; delPer=0; PSINO=7; BDRCVFR[C0p6oIjvx-c]=YaW3xZxuu8DfAF9pywdIAqsnH0; BDRCVFR[feWj1Vr5u3D]=I67x6TjHwwYf0; H_PS_PSSID=38186_36548_37518_38092_37910_38144_38267_38175_38170_38227_36803_38035_37926_37900_26350_38281_37881; Hm_lvt_d101ea4d2a5c67dab98251f0b5de24dc=1677586266; Hm_up_d101ea4d2a5c67dab98251f0b5de24dc={"uid_":{"value":"2438624082","scope":1}}; bdindexid=fu8ag4513mdb0qfctng01oivq5; SIGNIN_UC=70a2711cf1d3d9b1a82d2f87d633bd8a04275987188Gf8Hpic0DEeQqlAkvH02PIL/kedgCe1a/5Da9cmoC7HQKdwOo2UpFSLiMMCN6ZiD79vwiXE6+tQyQgBazU3B5UGcql5Ozxi4XmQhKzs6BD7VqVYcEZQZg7gZbwiIU67L15EQ+mY5hWIVXvVywHTZoszVhWBrCaWs/6/+58NjRJHhn9HozJYMSsbX/b77PWW/lH1AiXCzh8yI7q30gP9dEgS/5cpTGQQRHWI2j1CDSjJ6uM8CoiljGH/aDMvcPrKbO+4DIkIh5lmSxV2hD6fhykMCAFrh59/iMP0m9EfPRW8=71209201397139858273803164105523; __cas__rn__=427598718; __cas__st__212=71016b826c2d53e3f01c78a0b7ea9017ebba0170c31a49d1fdeed7f2d631810a61e0c96cc1199595e5251a15; __cas__id__212=45755999; CPID_212=45755999; CPTK_212=1811837657; Hm_lpvt_d101ea4d2a5c67dab98251f0b5de24dc=1677586267; ab_sr=1.0.1_ZGNhYzg0Y2NiZjYyMDYyY2ViNjNkNjY4ODE4YTQ2NDI3ZTM0NTA5ZjEyNGZkNmUzN2I5MTgxMGQzYmI0ODRkOTQyODk0NDhjZmYwNjc3NGQ1YmEzMWE3YjU5ZGZlN2FhZjYxNmMxZTI3OTI4NDcyZGQ5MzE2NDA5ZjM4Mzk5OTE3MDUwNzk2OTAxZjNkZDYyOGMzYWVhMjJiMWVhZTM2Yg==; BDUSS_BFESS=XU1QzJja3pGV0pNdHBGQUlrRUxEdnBHT3JOcWQyRlJ-Z2ZMczBDcldncWhaYVJqRVFBQUFBJCQAAAAAAAAAAAEAAABSc1qRbWFwZG9ntcQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKHYfGOh2Hxje; BCLID=11733474131775732123; BCLID_BFESS=11733474131775732123; BDSFRCVID=CckOJexroG0wAwcjpSjlhJGeQcpWxY5TDYrEOwXPsp3LGJLVc4HAEG0Pts1-dEu-S2EwogKK3mOTH4KF_2uxOjjg8UtVJeC6EG0Ptf8g0M5; BDSFRCVID_BFESS=CckOJexroG0wAwcjpSjlhJGeQcpWxY5TDYrEOwXPsp3LGJLVc4HAEG0Pts1-dEu-S2EwogKK3mOTH4KF_2uxOjjg8UtVJeC6EG0Ptf8g0M5; H_BDCLCKID_SF=tRAOoC8-fIvEDRbN2KTD-tFO5eT22-us5KFO2hcHMPoosIJ6LJJjMR89MG73Bfb-LC7j--nwJxbUotoHXh3tMt_thtOp-CrpKbn75l5TtUJMqIDzbMohqqJXQqJyKMnitKj9-pP2WhQrh459XP68bTkA5bjZKxtq3mkjbPbDfn028DKuDj-WDjJXeaRabK6aKC5bL6rJabC3eKt9XU6q2bDeQN3f3Rb22jRgBDnSWq3qVJOx3n7Zjq0vWq54WbbvLT7johRTWqR4HIbSLfonDh83KNLLKUQtHGAHK43O5hvvhn6O3MAMyUKmDloOW-TB5bbPLUQF5l8-sq0x0bOte-bQXH_EJ6tOtRAHVIvt-5rDHJTg5DTjhPrM5aLjWMT-MTryKKJ5043jj4JbMxosyx-80fbjLbvkJGnRh4oNB-3iV-OxDUvnyxAZ-NO73xQxtNRJQKDE5p5hKf84QJJobUPUDUc9LUkJW2cdot5yBbc8eIna5hjkbfJBQttjQn3hfIkj2CKLtC8WhD84e5RDKICV-frb-C62aKDs5bOoBhcqJ-ovQT3bLJ-SMl3gat73Wev-bROMQ-nZMfbeWJ5pXn-R0hbjJM7xWeJp3Kbd0l5nhMJmKTLVbML0qJ-H0Rby523iob6vQpPMOpQ3DRoWXPIqbN7P-p5Z5mAqKl0MLPbtbb0xXj_0-nDSHH-etj_q3j; H_BDCLCKID_SF_BFESS=tRAOoC8-fIvEDRbN2KTD-tFO5eT22-us5KFO2hcHMPoosIJ6LJJjMR89MG73Bfb-LC7j--nwJxbUotoHXh3tMt_thtOp-CrpKbn75l5TtUJMqIDzbMohqqJXQqJyKMnitKj9-pP2WhQrh459XP68bTkA5bjZKxtq3mkjbPbDfn028DKuDj-WDjJXeaRabK6aKC5bL6rJabC3eKt9XU6q2bDeQN3f3Rb22jRgBDnSWq3qVJOx3n7Zjq0vWq54WbbvLT7johRTWqR4HIbSLfonDh83KNLLKUQtHGAHK43O5hvvhn6O3MAMyUKmDloOW-TB5bbPLUQF5l8-sq0x0bOte-bQXH_EJ6tOtRAHVIvt-5rDHJTg5DTjhPrM5aLjWMT-MTryKKJ5043jj4JbMxosyx-80fbjLbvkJGnRh4oNB-3iV-OxDUvnyxAZ-NO73xQxtNRJQKDE5p5hKf84QJJobUPUDUc9LUkJW2cdot5yBbc8eIna5hjkbfJBQttjQn3hfIkj2CKLtC8WhD84e5RDKICV-frb-C62aKDs5bOoBhcqJ-ovQT3bLJ-SMl3gat73Wev-bROMQ-nZMfbeWJ5pXn-R0hbjJM7xWeJp3Kbd0l5nhMJmKTLVbML0qJ-H0Rby523iob6vQpPMOpQ3DRoWXPIqbN7P-p5Z5mAqKl0MLPbtbb0xXj_0-nDSHH-etj_q3j; RT="z=1&dm=baidu.com&si=f03c7aa1-f98e-4f69-ae26-d83e479659e1&ss=leo7jqkq&sl=2&tt=270&bcn=https://fclog.baidu.com/log/weirwood?type=perf"'


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

        ["自杀"],["上吊"],["百草枯"],["跳楼"],["安眠药"],

    ]
# ,["窒息"],["烦恼"],["神经衰弱"],["圣经"],["佛经"],["耶稣"]
# ["割腕"],["失业"],["抑郁"],["焦虑"]
#["自杀"],["上吊"],["百草枯"],["跳楼"],["安眠药"],

# ["安乐死"],["失眠"],["压力"],["忧虑"],["自缢"],
        # 
    for k in cn.CITY_CODE:
        get_search_index_demo(keywords_list,cn.CITY_CODE[k],keywords_list[0][0])
        print("{} finished".format(k))

    
