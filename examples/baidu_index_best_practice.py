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
sys.path.insert(0,r'C:\Local Storage\Code\spider-BaiduIndex\qdata\baidu_index')
import config as cn


cookies = 'CHKFORREG=71cff3ebc3aac37b93a7a7e5206f516a; BIDUPSID=09370DE5A9DDC0813960B31A6BB1F1F6; PSTM=1676622284; ZFY=gOMNPR0ooI:AqTp1:ASSIl4jSb6xCgLvCIV2FpHwQnC1A:C; BAIDUID=09370DE5A9DDC081BD141692AA73B85D:SL=0:NR=10:FG=1; BAIDUID_BFESS=09370DE5A9DDC081BD141692AA73B85D:SL=0:NR=10:FG=1; BDUSS=EhOa0JpTjljNmVuN3lGRy1uNFlJR3M1d05hUUxibU5yMm5VRXhpLVlqUngyeFprSVFBQUFBJCQAAAAAAAAAAAEAAADNg4yS1uy78s7SAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHFO72NxTu9jS; __bid_n=1867d49b83657c29864207; FPTOKEN=2vyfWU7MuGAA8Q/C0jNHkEOJT7ubuB5GhtiKNuE5bhQKmt72hVbFc/E0li60GTvukntC+dY9/dfITLk0BRVg3uS3KM9O2TbO24+MgTBtN7xGAEOoEGxQJe02XCasBxk5O91F9ddXK55d0LeEpIsFmYoBdBY61xEDOkPKCMa28rZigCVlzAalXqqBIicUSnRiP6Ajg16iFzaNLFPys9Ey4azvyXDvVFhsg+Q9O1ytt+2dzhyVxvHZfBQJoLTDMNWB8sn7IbdH4KP+H8FRjciF+RWHh2q7DNMcxJCJXLUGEQE01ImgkbpEBfghk8WdfPCRu4rqXHi30VLAu5xrIUWw+RIhP2GjLdn/wxCQk08rZXW/Bh2cPy48WUN8B7pbx/4RbsbEA5l5WXUi/lQ1o2s1Fw==|lwtUsyhH/mTq4Coj8ewuJPS2Qa8x0ZV0xEQH69okleU=|10|f71771bac2954a2fa71ae0d63e43f3c8; Hm_up_d101ea4d2a5c67dab98251f0b5de24dc={"uid_":{"value":"2458682317","scope":1}}; bdindexid=spaafvitrc4fh8cojhu72bc4e5; SIGNIN_UC=70a2711cf1d3d9b1a82d2f87d633bd8a04274549799B+8IlzKmC64kNo3yvGq4DO0njcGwmY9a/KORRL3doI3X4SmwZ7gtaiXseZjv+H7dq1cMa1RFScpm6S6h5bPKWye9kax7MZurGu/tE5oO1BA2DL3BoruRqRTYk59pW/u9XqrsBGExbVfSaqfKqAmu7XtUb4DEtUMP462gJSiyGC+wDBcbGisG1cMod8h2sk1c651RHdJPZTy4bRuv/HGAk/KeLA1kfkILtwKU+cz0zwl1xf9b+h0BH7ZBm3H942nBNPZx4rI/AIze0+AiH+LXeYHCe/oyG2iqtcMiafg/0Fs=94788329343491631128895402927148; __cas__rn__=427454979; Hm_lvt_d101ea4d2a5c67dab98251f0b5de24dc=1677290005,1677291142,1677442533; __cas__st__212=9a7e488a17d2447ae0be918dc9f31e2ec185b2d0f77172465525f11f1f469c2063e7922e5cf23632c381dd8f; __cas__id__212=45681365; CPID_212=45681365; CPTK_212=100567939; ab_sr=1.0.1_MDk2NTU3ZmJmOWVjYzg3OWQ3OWI3MDU3ZTA3NTNmYTJkZThjYzFhYmMzYzY4ZWYzNTU2NTYzY2RiYjdlODc1ZWU3Y2NmNmQzYmM5ZjQxOGNjMzYxOWZjYTEyMTE2OTk0MmVlMDM1MGUyYWYwYTZiMWYzMTBhODVkODk4MTgxYTcxNWQ1MzJkMDNmZTY4YjY5YjNiMGNhMjAzZTUyYzU2NQ==; Hm_lpvt_d101ea4d2a5c67dab98251f0b5de24dc=1677443395; BDUSS_BFESS=EhOa0JpTjljNmVuN3lGRy1uNFlJR3M1d05hUUxibU5yMm5VRXhpLVlqUngyeFprSVFBQUFBJCQAAAAAAAAAAAEAAADNg4yS1uy78s7SAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHFO72NxTu9jS; RT="z=1&dm=baidu.com&si=d17b8b4b-d4b1-4df9-a676-12de156cfae2&ss=leltyyd1&sl=a&tt=l0r&bcn=https://fclog.baidu.com/log/weirwood?type=perf"'


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
    pd.DataFrame(datas).to_excel("C:\Local Storage\Code\spider-BaiduIndex\DownloadedData\{}_{}_index.xlsx".format(kw,area))


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
                end_date='2022-12-31',
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
        ["自杀"],["上吊"],["百草枯"]
    ]

    for k in cn.CITY_CODE:
        get_search_index_demo(keywords_list,cn.CITY_CODE[k],keywords_list[0][0])
        print("{} finished".format(k))

    
