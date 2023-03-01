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


cookies = 'CHKFORREG=fc549fecbadc5f537975d933f98619b9; BIDUPSID=5A72650EA68EB5C064DE2F03BDF3B2D7; PSTM=1646991284; BAIDUID=5A72650EA68EB5C0C56CCEDB500A1787:FG=1; BDUSS=htVWdoaUFrQzlPUnZta2N5MVJvNDRFS0x5V085S21NUFBGLX5XODMtcGEtV2xpSVFBQUFBJCQAAAAAAAAAAAEAAAARzdETztKyu8rHsqPBp8W20rIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFpsQmJabEJiOU; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; MCITY=-:; BA_HECTOR=8k2ga4850g85a4052520al1o1hvqqsn1k; delPer=0; BAIDUID_BFESS=5A72650EA68EB5C0C56CCEDB500A1787:FG=1; ZFY=hGt9QTpC4dxYTMpH7CA6jLwwkNFzZxTZBJQpWGW0f4A:C; PSINO=6; H_PS_PSSID=38186_36561_37517_38113_38091_37906_38144_38266_38178_38172_36807_37930_38089_38041_26350_38136_22159_38282_37881; Hm_lvt_d101ea4d2a5c67dab98251f0b5de24dc=1677559831; Hm_up_d101ea4d2a5c67dab98251f0b5de24dc={"uid_":{"value":"332516625","scope":1}}; bdindexid=7jko0p87v6c4b95fjd98l92cf0; SIGNIN_UC=70a2711cf1d3d9b1a82d2f87d633bd8a042757228225c80DFFKELUzx4BFNvK/rjauBg0xnw4tZWWGxfwi+BEB6hnIHyIl7lg0jofg/ZPhrSrCHWXzTwF4C8Ca7aEW3n/tytK/gIH+9E2quG0byXMiKssPSCfy90eW6ITUeh8FeS679G3h/MLiuHR354e61eMNp58mjt+IHe0uo2ZsSSB6RI9tIb3LFL79/nNmo2xfNqznl2YO8oaIQQ7vkwkirXcEH2kFE5kHVKlsX13/ORKjJrUdiGbHLv2a8z/uzNBa9SNcvfVK6rKUWdnQVo/jZ7aFJTOq0YlnH48fTqF8uDjV8dWCuhuHuAoHD8wxwkdhWwtJx4zBdIKP367+NjxuB4LBcn9HJ9GCkHlWvBIemK8=20370012324356924629559901211851; __cas__rn__=427572282; __cas__st__212=3501482f55a86f7ce0c6bb7b2d3c1d9af10bf1171394b4fd1f0aa727d0a15c868aa790bfa6b32c4dd28ccde2; __cas__id__212=45739814; CPID_212=45739814; CPTK_212=1182186504; Hm_lpvt_d101ea4d2a5c67dab98251f0b5de24dc=1677559834; BDUSS_BFESS=htVWdoaUFrQzlPUnZta2N5MVJvNDRFS0x5V085S21NUFBGLX5XODMtcGEtV2xpSVFBQUFBJCQAAAAAAAAAAAEAAAARzdETztKyu8rHsqPBp8W20rIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFpsQmJabEJiOU; ab_sr=1.0.1_YjA5YWM4MGE4YzYxOGI2MmM3NjdmOTgyYjY0NDk0NDY4ODBjODdlNjBiZGNlMDY2NmEyMzllMjRiYTMwMDk2OWM5MGMwNjUyOTg4ODY4ZGI2ZTkwNDNjMmRjZGFkN2Q1OGU1OTg5YTIzN2QyYzMxNWM0NjFlY2JiMzhlN2Q3MGE1NzViM2Q0MjA5MThmMTE1ZjM0NzNjZmU0NjY5NjczYg==; RT="z=1&dm=baidu.com&si=4d63f629-6eb5-4636-a454-14e36a3336a9&ss=lenrt551&sl=2&tt=2uz&bcn=https://fclog.baidu.com/log/weirwood?type=perf"'


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
        ["耶稣"]
        # ["窒息"],["烦恼"],["神经衰弱"],["圣经"],["佛经"],

    ]
# ["安乐死"],["失眠"],["压力"],["忧虑"],["自缢"],

# ,

#["自杀"],["上吊"],["百草枯"],["跳楼"],["安眠药"],

        # ["割腕"],["失业"],["抑郁"],["焦虑"]
    for k in cn.CITY_CODE2:
        get_search_index_demo(keywords_list,cn.CITY_CODE2[k],keywords_list[0][0])
        print("{} finished".format(k))

    
