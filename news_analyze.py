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
import cloudscraper
import nltk
from nltk import word_tokenize     #以空格形式实现分词
#nltk.download()
from nltk.corpus import PlaintextCorpusReader
import spacy


'''
corpus_root = r'D:\ztrade\美股基本数据'  # 目标文件路径
wordlists = PlaintextCorpusReader(corpus_root, r'nameUS.txt')  # 匹配加载想要的txt文件
wordlists.fileids()
raw = wordlists.raw(r'D:\ztrade\美股基本数据\nameUS.txt')
raw
word = wordlists.words()

pass.
'''

scraper = cloudscraper.create_scraper()

plt.rcParams['font.sans-serif'] = 'SimHei'  # 显示中文
plt.rcParams['axes.unicode_minus'] = False  # 显示负号

'''
with open("D:\ztrade\A股数据\申万行业分类词典.txt",'r') as f:
    content = f.read()
content1=content.replace('\t', ' ')
pass
#content1=content.encode('utf-8').decode("unicode_escape")
pass
with open('D:\ztrade\A股数据\申万行业分类dict.txt','w',encoding='utf-8') as f1:
   f1.write(content1)
pass
'''


#A股名称列表
f = open('D:\ztrade\A股数据\codedict.csv', 'r')
csvreader = csv.reader(f)
codeList = list(csvreader)
codeList = list(itertools.chain.from_iterable(codeList))
f.close()

#加载行业词典
with open(r"D:\ztrade\A股数据\申万行业分类dict.txt",'r',encoding='utf-8') as f:
    content = f.read()
content=content.split('\n')
f.close()
industrialList=content

'''
#获取金融情绪词典
df=pd.read_excel("D:\ztrade\中文金融情感词典_姜富伟等(2020).xlsx",sheet_name=['positive','negative'])
positiveWord=df['positive']['Positive Word']
negativeWord=df['negative']['Negative Word']
'''
#加载新的情绪词典
with open(r"D:\ztrade\中文金融词典\dict\formal_pos.txt",'r',encoding='utf-8') as f:
    content = f.read()
content=content.split('\n')
f.close()

with open(r"D:\ztrade\中文金融词典\dict\unformal_pos.txt",'r',encoding='utf-8') as f:
    content1 = f.read()
content1=content1.split('\n')
f.close()
positiveList=content+content1

with open(r"D:\ztrade\中文金融词典\dict\formal_neg.txt",'r',encoding='utf-8') as f:
    content = f.read()
content=content.split('\n')
f.close()
with open(r"D:\ztrade\中文金融词典\dict\unformal_neg.txt",'r',encoding='utf-8') as f:
    content1 = f.read()
content1=content1.split('\n')
f.close()
negativeList=content+content1

#读取美股的名称列表
nameListDataFrame = pd.read_csv(r"D:\ztrade\美股基本数据\CodeNameUS.csv")#指定读取第一个sheet
nameListUS=nameListDataFrame['nameshort'].tolist()


pass
#content1=content.encode('utf-8').decode("unicode_escape")
pass

#positiveList=positiveWord.tolist()
#negativeList=negativeWord.tolist()

allList=positiveList+negativeList
'''
with open("D:\ztrade\A股数据\情绪词汇.txt", "w",encoding='utf-8') as fpn:
    str = ''
    strJoin=str.join(allList)
    fpn.writelines(strJoin)
f.close()
'''
pass

# 定义爬取函数
def crawl_sina_finance_reports(pages=300):
    print('爬取新浪财经数据')

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
        print(f'爬取第{page}页')

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

# 定义爬取函数
def crawlInvestingFinanceReports(pages=50):
    print('爬取英为才情数据')
    base_url = "https://investing.com/news/stock-market-news"
    reports = []
    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    }
    titles = []
    for page in range(1, pages):
        #url = f"{base_url}{page}"
        if page==1:
            pageStr=''
        else:
            pageStr='/'+str(page)

        print(f'爬取第{page}页')
        url = base_url+pageStr

        response=scraper.get(url) # => "<!DOCTYPE html><html><head>..."

        #response = requests.get(url, headers=headers)  #
        # 使用chardet检测编码
        detected_encoding = chardet.detect(response.content)['encoding']
        if detected_encoding:
            # print(detected_encoding)
            response.encoding = detected_encoding
        else:
            response.encoding = 'GB2312'  # 如果chardet无法检测到编码，则默认使用GB2312
        soup = BeautifulSoup(response.content)  # , 'html.parser'

        # 找到所有报道的列表项
        report_items = soup.find_all("a",class_="text-inv-blue-500 hover:text-inv-blue-500 hover:underline focus:text-inv-blue-500 focus:underline whitespace-normal text-sm font-bold leading-5 !text-[#181C21] sm:text-base sm:leading-6 lg:text-lg lg:leading-7")  #

        for item in report_items:
            title = item.text.strip()
            titles.append(title)

    return titles



def crawStockName():
    base_url = "https://statementdog.com/us-stock-list"
    reports = []
    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    }
    titles = []
    url = base_url
    suffixs=[' Inc',' Corp',' Co',' Co.',' SA',' PLC',' plc',' Ltd',' NV',' SA',' SE']

    response = requests.get(url, headers=headers)  #

    #response = requests.get(url, headers=headers)  #
    # 使用chardet检测编码
    detected_encoding = chardet.detect(response.content)['encoding']
    if detected_encoding:
        # print(detected_encoding)
        response.encoding = detected_encoding
    else:
        response.encoding = 'GB2312'  # 如果chardet无法检测到编码，则默认使用GB2312
    soup = BeautifulSoup(response.content)  # , 'html.parser'

    df=pd.DataFrame(columns=['code','name','nameshort'])
    report_items = soup.find_all("a",class_="us-stock-company")
    for item in report_items:
        raw = item.text.strip()
        codeName=raw.split(') ',1)
        code=codeName[0].replace('(','')
        name = codeName[1]
        nameshort=name
        for suffix in suffixs:
            str = nameshort.replace(suffix,'')
            nameshort = str

        df.loc[len(df.index)]=[code,name,nameshort]

    df.to_csv('CodeNameUS.csv')
    return titles


def titleEmotionAnalyze(dataframe):
    #取最多的100个
    maxTitles=100
    titles = dataframe['标题']
    emotionCount = pd.DataFrame(columns=['code', 'positive', 'negative','result'])
    emotionCountDict={}
    jieba.load_userdict(r"D:\ztrade\A股数据\Acode.txt")
    #情绪词汇加载
    jieba.load_userdict(r"D:\ztrade\A股数据\情绪词汇.txt")

    #titles=['成都银行(601838)：营收利润增速保持强劲 不良率持续下降','成都银行(601838)：营收利润增速保持强劲 不良率持续下降']
    #titles=
    for title in titles:
        positiveCount = 0
        negativeCount = 0
        seg_list = jieba.cut(title, cut_all=False)
        word_list = list(seg_list)
        wordArry = np.array(word_list)

        # 判断分词是否有code
        codeIsInList=np.isin(word_list, codeList, invert=False)
        if any(codeIsInList):
            #先把code提取出来：
            code=wordArry[codeIsInList][0]
            #结果表中是否有，没有新建一行
            if code in list(emotionCount['code']):
                index = emotionCount[emotionCount['code'] == code].index[0]
            else:
                # 新起一行
                emotionCount.loc[len(emotionCount['code'])] = [code, 0, 0, 0]
                index=-1
            #开始遍历数据
            for word in word_list:
                # 找出code
                if word == code:
                    continue
                    #找出是否在dataframe已存在
                # 找出积极词：
                if word in positiveList:
                    positiveCount += 1
                # 找出消极次
                if word in negativeList:
                    negativeCount += 1
            emotionCount['positive'].iloc[index] += positiveCount
            emotionCount['negative'].iloc[index] += negativeCount
                #emotionCountDict[code]=positiveCount-negativeCount
    emotionCount['result']=emotionCount['positive']-emotionCount['negative']
    emotionCount = emotionCount.sort_values(by="positive", ascending=False)
    if len(emotionCount['code'])>100:
        emotionCount=emotionCount.iloc[:99,]
    for i in range(0,len(emotionCount['code'])):
        code=emotionCount['code'].iloc[i]
        emotionCountDict[code]=emotionCount['positive'].iloc[i]
    pass
    return emotionCountDict

def titleIndustAnalyze(dataframe):
    #取最多的100个
    maxTitles=100
    titles = dataframe['标题']
    all_titles = ' '.join(df['标题'])
    jieba.load_userdict(r"D:\ztrade\A股数据\Acode.txt")
    #情绪词汇加载
    jieba.load_userdict(r"D:\ztrade\A股数据\申万行业分类dict.txt")
    seg_list = jieba.cut(all_titles, cut_all=False)
    # 添加词性标致
    seg_text = ' '.join(seg_list)
    # 对分词文本做高频词统计
    word_counts = Counter(seg_text.split())
    word_counts_updated = word_counts.most_common()
    # 过滤标点符号
    # non_chinese_pattern = re.compile(r'[^\u4e00-\u9fa5]')
    filtered_word_counts_regex = [item for item in word_counts_updated if item[0] in industrialList]
    if len(filtered_word_counts_regex)>100:
        filtered_word_counts_regex= filtered_word_counts_regex[0:99]

    return filtered_word_counts_regex

def titleCountUS(titles):
    print("统计词频")
    #取最多的100个
    maxTitles=100
    #情绪词汇加载
    titlesStr=''
    for title in titles:
        titlesStr+=title+'.'
    #words = word_tokenize(titlesStr)
    jieba.load_userdict(r"D:\ztrade\美股基本数据\nameUS.txt")

    seg_list = jieba.cut(titlesStr, cut_all=False)
    word_list = list(seg_list)
    wordArry = np.array(word_list)
    nameList=[]
    for word in word_list:
    # 判断分词是否有code
        if word in nameListUS:
            nameList.append(word)

    #word_list = list(words)
    #wordArry = np.array(words)
    word_counts = Counter(nameList)
    word_counts_updated = word_counts.most_common()

    if len(word_counts_updated)>100:
        word_counts_updated= word_counts_updated[0:99]
    return word_counts_updated
    # 判断分词是否有code
    #if any(codeIsInList):

def titleCountUSBySpacy(titles):
    print("统计词频bySpacy")

    #取最多的100个
    maxTitles=100
    #情绪词汇加载
    nameList=[]
    titlesStr=''
    for title in titles:
        titlesStr+=title+'.'
    #words = word_tokenize(titlesStr)
    NER = spacy.load("en_core_web_sm", disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
    text = NER(titlesStr)
    spacy.displacy.render(text, style="ent", jupyter=True)
    for w in text.ents:
        print(w.text, w.label_)
        if  w.label_ == 'ORG':
            nameList.append(w.text)
    word_counts = Counter(nameList)
    word_counts_updated = word_counts.most_common()

    if len(word_counts_updated) > 100:
        word_counts_updated = word_counts_updated[0:99]
    return word_counts_updated
    '''
    seg_list = jieba.cut(titlesStr, cut_all=False)
    word_list = list(seg_list)
    wordArry = np.array(word_list)
    for word in word_list:
    # 判断分词是否有code
        if word in nameListUS:
            nameList.append(word)

    #word_list = list(words)
    #wordArry = np.array(words)
    word_counts = Counter(nameList)
    word_counts_updated = word_counts.most_common()

    if len(word_counts_updated)>100:
        word_counts_updated= word_counts_updated[0:99]
    return word_counts_updated
    # 判断分词是否有code
    #if any(codeIsInList):
    '''

# 爬取数据
reports_data = crawl_sina_finance_reports()
#crawlInvestingFinanceReports()
#crawStockName()
titles=crawlInvestingFinanceReports()
nameCount=titleCountUSBySpacy(titles)
countDict=dict(nameCount)
# 创建DataFrame
#df_reports = pd.DataFrame(reports_data, columns=["标题", '报告类型', "发布日期", "机构"])
#df_reports

#df_reports.to_csv('财经新闻.csv',index=False)  #储存

#df=df_reports.copy()


#报道标题情绪识别
#emotionCountDict=titleEmotionAnalyze(df)
#countDict=dict(titleIndustAnalyze(df))
#print(emotionCountDict)

'''
all_titles = ' '.join(df['标题'])
# Word segmentation
jieba.load_userdict(r"D:\ztrade\A股数据\Acode.txt")
jieba.load_userdict(r"D:\ztrade\A股数据\情绪词汇.txt")
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
'''
# Generate word cloud
print("绘制词频图")
wordcloud = WordCloud(font_path='simhei.ttf', background_color='white',
                      max_words=100,  # Limits the number of words to 100
                      max_font_size=50)  # .generate(seg_text)    #文本可以直接生成，但是不好看
wordcloud = wordcloud.generate_from_frequencies(countDict)
# Display the word cloud
plt.figure(figsize=(8, 5), dpi=256)
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()



pass
