# 本代码用于把文本评论导入数据库 而且是一个 SQl Server 数据库
# 其他数据库应修改相应部分，但是前面提取数据是一致的

import datetime
import pyodbc
import html
import time
import sys

server = 'localhost'
database = 'ETL_ass_1'
username = 'zzj'
password = 'zzjHH'

cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

file = open('movies.txt', 'r', encoding='utf-8', errors='ignore')
finished = False
count = 0
start_time = time.time()
while not finished:
    count += 1
    product_id = ''
    user_id = ''
    profile_name = ''
    helpfulness = ''
    score = 0.0
    review_time = None
    summary = ''
    text = ''
    # 从文本文件中读出数据
    while True:
        line = file.readline()
        # print(line)
        if line.find('product/productId') == 0:
            product_id = line.replace('product/productId: ', '').replace('\n', '')
        elif line.find('review/userId') == 0:
            user_id = line.replace('review/userId: ', '').replace('\n', '')
        elif line.find('review/profileName') == 0:
            profile_name = html.unescape(line.replace('review/profileName: ', '').replace('\n', ''))
        elif line.find('review/helpfulness') == 0:
            helpfulness = line.replace('review/helpfulness: ', '').replace('\n', '')
        elif line.find('review/score') == 0:
            score = float(line.replace('review/score: ', '').replace('\n', ''))
        elif line.find('review/time') == 0:
            review_time = datetime.datetime.fromtimestamp(int(line.replace('review/time: ', '').replace('\n', '')))
        elif line.find('review/summary') == 0:
            summary = html.unescape(line.replace('review/summary: ', '').replace('\n', ''))
        elif line.find('review/text') == 0:
            text = html.unescape(line.replace('review/text: ', '').replace('\n', ''))
        elif line.find('\n') == 0:
            break
        elif line == '':
            finished = True
            break
    # 下面开始是存数据库，应该进行修改
    if product_id != '' and user_id != '':
        try:
            cursor.execute("""
                    insert into movie_review(product_id, user_id, profile_name, helpfulness, score, review_time, summary, review_text)
                    values (?,?,?,?,?,?,?,?)
            """, [
                product_id, user_id, profile_name, helpfulness, score, review_time, summary, text
            ])
        except:
            print(sys.exc_info()[0])
    if count % 10000 == 0:
        cnxn.commit()
        print('inserted: #', count, 'Time spent:', time.time() - start_time)

file.close()
cursor.close()
cnxn.close()
