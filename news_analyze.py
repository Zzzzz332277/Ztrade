import itertools

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from wordcloud import WordCloud
import jieba
import jieba.posseg as pseg
import re
import chardet
import csv


plt.rcParams['font.sans-serif'] = 'SimHei'  # 显示中文
plt.rcParams['axes.unicode_minus'] = False  # 显示负号

'''
with open("D:\ztrade\A股数据\工作簿3.txt",'r') as f:
    content = f.read()
content1=content.replace('\t', ' ')
pass
#content1=content.encode('utf-8').decode("unicode_escape")
pass
with open('D:\ztrade\A股数据\工作簿4.txt','w',encoding='utf-8') as f1:
   f1.write(content1)
pass
'''
#A股名称列表
f = open('D:\ztrade\A股数据\codedict.csv', 'r')
csvreader = csv.reader(f)
codeList = list(csvreader)
codeList = list(itertools.chain.from_iterable(codeList))

#获取金融情绪词典
df=pd.read_excel("D:\ztrade\中文金融情感词典_姜富伟等(2020).xlsx",sheet_name=['positive','negative'])
positiveWord=df['positive']['Positive Word']
negativeWord=df['negative']['Negative Word']

positiveList=positiveWord.tolist()
negativeList=negativeWord.tolist()

allList=positiveList+negativeList

with open("D:\ztrade\A股数据\情绪词汇.txt", "w",encoding='utf-8') as fpn:
    str = ''
    strJoin=str.join(allList)
    fpn.writelines(strJoin)
f.close()

pass




pass
# 定义爬取函数
def crawl_sina_finance_reports(pages=300):
    base_url = "https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/lastest/index.phtml"
    reports = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    for page in range(1, pages + 1):
        url = f"{base_url}?p={page}"
        response = requests.get(url, headers=headers)  #
        # 使用chardet检测编码
        detected_encoding = chardet.detect(response.content)['encoding']
        if detected_encoding:
            # print(detected_encoding)
            response.encoding = detected_encoding
        else:
            response.encoding = 'GB2312'  # 如果chardet无法检测到编码，则默认使用GB2312
        soup = BeautifulSoup(response.content)  # , 'html.parser'

        # 找到所有报道的列表项
        report_items = soup.find_all('tr')[1:]  # 跳过表头
        for item in report_items:
            columns = item.find_all('td')
            if len(columns) >= 4:
                title = columns[1].text.strip()
                kind = columns[2].text.strip()
                date = columns[3].text.strip()
                organization = columns[4].text.strip()
                reports.append([title, kind, date, organization])

    return reports


# 爬取数据
reports_data = crawl_sina_finance_reports()

# 创建DataFrame
df_reports = pd.DataFrame(reports_data, columns=["标题", '报告类型', "发布日期", "机构"])
df_reports

#df_reports.to_csv('财经新闻.csv',index=False)  #储存

df=df_reports.copy()

# Analysis 4: Word cloud of titles
all_titles = ' '.join(df['标题'])
# Word segmentation
jieba.load_userdict(r"D:\ztrade\A股数据\工作簿4.txt")
#jieba.add_word("阳光电源")

seg_list = jieba.cut(all_titles, cut_all=False)
#添加词性标致


seg_text = ' '.join(seg_list)
# 对分词文本做高频词统计
word_counts = Counter(seg_text.split())
word_counts_updated = word_counts.most_common()
# 过滤标点符号
#non_chinese_pattern = re.compile(r'[^\u4e00-\u9fa5]')

#匹配数字的正则表达式
#digit_pattern = re.compile(r'\d{6}')

# 过滤掉非中文字符的词汇
#filtered_word_counts_regex = [item for item in word_counts_updated if not non_chinese_pattern.match(item[0])]
#filtered_word_counts_regex=filtered_word_counts_regex[7:]
#filtered_word_counts_regex = [item for item in word_counts_updated if  digit_pattern.match(item[0])]
#判断是否在字典中：
filtered_word_counts_regex = [item for item in word_counts_updated if item[0] in codeList]

# Generate word cloud
wordcloud = WordCloud(font_path='simhei.ttf', background_color='white',
                      max_words=80,  # Limits the number of words to 100
                      max_font_size=50)  # .generate(seg_text)    #文本可以直接生成，但是不好看
wordcloud = wordcloud.generate_from_frequencies(dict(filtered_word_counts_regex))
# Display the word cloud
plt.figure(figsize=(8, 5), dpi=256)
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()



pass
