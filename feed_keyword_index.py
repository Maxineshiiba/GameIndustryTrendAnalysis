#!/usr/bin/env python
#-*- coding: utf-8 -*-

# Objective: to analyze the leverage ratio of external factors which would affect game performance 
# in the market, thereby facilitating the execution of the marketing process in a more cost-effective way

#Logic for current model: Keyword Index is calculated by using the document frequency(DF) of a keyword in the 10 past days, and then attenuate
#the DF value (current day has the largest weight, decay with time), and finally sum all the DF values


import json,os,string,datetime,time,getopt,math
import sys
parentdir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,parentdir)
from BIReport.BI_Engine import *
import numpy as np
import pandas as pd
reload(sys)
sys.setdefaultencoding('utf-8')


class Parameters():
    def __init__(self):
        self.epsilon = math.pow(10, -5)
        self.damp = 0.85
        self.weight_list = []
        for i in range(10):
            self.weight_list.append(math.exp(-i))
        self.weight_list.reverse()

def execute(date_begin, date_end):
    print 'date_begin=', date_begin, ' date_end=', date_end
    if date_begin > date_end:
        return 'FAILED: begin_date > end_date'

    begin_date = date_begin
    end_date = date_end

    para = Parameters()

    insert_list = []

    # print weight_list

    while begin_date <= end_date:
        insert_list_d = []
        date_10d_list = []
        for i in range(10):
            date_10d_list.append(int((datetime.datetime.strptime(str(begin_date), '%Y%m%d') + datetime.timedelta(days=-i)).strftime('%Y%m%d')))
        date_10d_list.reverse()

        begin_date_10d = int((datetime.datetime.strptime(str(begin_date), '%Y%m%d') + datetime.timedelta(days=-9)).strftime('%Y%m%d'))

        sql_1 = '''
                select publish_date, keyword, sum(text_cnt) text_cnt
                from (
                    select a.publish_date
                        ,regexp_split_to_table(keywords,';') as keyword
                        ,1 text_cnt
                    from spider_feed_wordlist_data a
                    where a.publish_date >= %s and a.publish_date <= %s 
                        and length(keywords)>0 
                        and a.is_game = 1
                ) a
                --where keyword='android'
                group by publish_date, keyword
        ''' % (begin_date_10d, begin_date)

        data_1 = Data(
            fields=[Field(code='publish_date'), Field(code='keyword'), Field(code='text_cnt')],
            rows=Model('pg', sql_1).getSelect())

        sql_keywords = '''
                select distinct keyword
                from (
                    select a.publish_date
                        ,regexp_split_to_table(a.keywords,';') as keyword
                    from spider_feed_wordlist_data a
                    where a.publish_date >= %s and a.publish_date <= %s 
                        and length(a.keywords)>0
                        and a.is_game = 1
                ) a
                group by keyword
        ''' % (begin_date_10d, begin_date)

        keywords = Data(fields=[Field(code='keyword')], rows=Model('pg', sql_keywords).getSelect())

        # page_rank
        keywords_list, PR = text_rank(keywords, date_10d_list)

        for i, keyword in enumerate(keywords_list):
            data_keyword = data_1.selectData('publish_date,text_cnt', '${keyword} == \"' + keyword + '\"')
            data_keyword.orderBy('publish_date')

            # print data_keyword

            text_cnt_list = []
            for date in date_10d_list:
                for i, r in enumerate(data_keyword.rows):
                    if r[0] == date:
                        text_cnt_list.append(r[1])
                        break
                    elif i == len(data_keyword.rows) - 1:
                        text_cnt_list.append(0)

            keyword_pr = PR[i]
            keyword_df = 0
            for i, w in enumerate(para.weight_list):
                keyword_df = keyword_df + w * text_cnt_list[i]

            # print begin_date, keyword.decode('utf-8'), text_cnt_list[9], keyword_df, math.exp(keyword_df)
            # if begin_date == 20181008 and keyword == '环世界':
            #     print begin_date, keyword.decode('utf-8'), text_cnt_list[9], keyword_df, math.exp(keyword_df)
            #     print text_cnt_list, data_keyword.rows, date_10d_list

            insert_list_d.append([begin_date, keyword, text_cnt_list[9], 0.8 * keyword_df + (1 - 0.8) * 200 * keyword_pr,
                                  math.pow(0.8 * keyword_df + 0.2 * 100 * keyword_pr, 1.5) * 545, keyword_pr, keyword_df])

            # break

        insert_list_d.sort(key=lambda x: (x[4]), reverse=True)
        for i, ii in enumerate(insert_list_d):
            ii.insert(2, i)

        insert_list.extend(insert_list_d)

        begin_date = int((datetime.datetime.strptime(str(begin_date), '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d'))


    delete_sql = 'delete from spider_feed_keyword_index where publish_date between %s and %s;' % (date_begin, date_end)
    insert_sql = ''
    for i in insert_list:
        insert_sql = insert_sql + "insert into spider_feed_keyword_index (publish_date, keyword, sort, df_1d, keyword_index, keyword_index_e, pr, df) values (%s, '%s', %s, %s, %s, %s, %s, %s);" \
                 % (i[0], i[1].replace("'", "''"), i[2], i[3], i[4], i[5], i[6], i[7])
    sql = delete_sql + insert_sql

    try:
        conn = DBHelper().getPGDB()
        cur = conn.cursor()

        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
    except:
        print sql

    # print sql
    return 'done'




def text_rank(keywords_all, date_10d_list):

    PR_all = np.ones((len(keywords_all.rows), len(date_10d_list))) / len(keywords_all.rows)
    PR_all_result = np.zeros(len(keywords_all.rows))

    keywords_list_all = []
    for row in keywords_all.rows:
        keywords_list_all.append(row[0])

    para = Parameters()

    sql = '''
        select publish_date, keywords
        from spider_feed_wordlist_data a
        where a.publish_date >= %s and a.publish_date <= %s 
            and length(keywords)>0 and a.keywords like '%%;%%'
            and a.is_game = 1
    ''' % (date_10d_list[0], date_10d_list[9])

    data = Data(fields=[Field(code='publish_date'), Field(code='keywords')], rows=Model('pg', sql).getSelect())

    df_data = pd.DataFrame(data.rows, columns=[a.getCode() for a in data.fields])

    for d, date in enumerate(date_10d_list):

        sql_keywords = '''
                select distinct keyword
                from (
                    select a.publish_date
                        ,regexp_split_to_table(a.keywords,';') as keyword                        
                    from spider_feed_wordlist_data a
                    where a.publish_date = %s 
                        and length(a.keywords)>0 and a.keywords like '%%;%%'
                        and a.is_game = 1
                ) a
                group by keyword
        ''' % (date)

        keywords = Data(fields=[Field(code='keyword')], rows=Model('pg', sql_keywords).getSelect())

        keywords_list = []
        for row in keywords.rows:
            keywords_list.append(row[0])

        n = len(keywords.rows)

        W = np.zeros((n, n))
        # print str(date), "Preprocessing start = ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for i, i_row in enumerate(keywords.rows):
            for j, j_row in enumerate(keywords.rows):
                if i > j:
                    i_or_j_cnt = len(
                        df_data.query('(keywords.str.contains(\"' + i_row[0] + '\") or keywords.str.contains(\"' + j_row[0] + '\")) and publish_date==' + str(date)))
                    if i_or_j_cnt > 0:
                        i_and_j_cnt = len(
                            df_data.query('keywords.str.contains(\"' + i_row[0] + '\") and keywords.str.contains(\"' + j_row[0] + '\") and publish_date==' + str(date)))
                        w_i_j = i_and_j_cnt * 1.0 / i_or_j_cnt
                        W[i, j] = w_i_j
                        W[j, i] = w_i_j
                        # if w_i_j > 0:
                        #     print i, str(date), i_row[0], j_row[0], i_j_cnt, '/', i_cnt, '=', w_i_j
        # print str(date), "Preprocessing end = ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # print 'keywords_list, W', json.dumps(keywords_list, encoding="UTF-8", ensure_ascii=False), W
        # print str(date), "PageRank training start = ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        index0 = np.where(np.max(W, axis=1) == 0)  
        # print 'indexs_full0 = ', str(index0)
        for i in index0:  
            W[i, :] = 1. / n
            W[:, i] = 1. / n

        # print 'date=', date, 'n=', n, datetime.datetime.now()
        # index_tmp = np.where(np.max(W, axis=1) != 0)

        PR = np.ones(n) / n  # pagerank score rank
        PR_last = np.zeros(n)  # ave the score from last time, used for final caculation after iteration is finished

        while np.max(abs(PR - PR_last)) > para.epsilon:
            PR_last = PR.copy()
            PR = (1 - para.damp) / n + para.damp * (PR / (np.sum(W, axis=1).T + para.epsilon)).dot(W)
            # print "np.max(abs(PR - PR_last) = ", str(np.max(abs(PR - PR_last)))
        # print "PR = ", str(PR)
        # print str(date), "PageRank training end = ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i, kw in enumerate(keywords_list):
            idx = keywords_list_all.index(kw)
            PR_all[idx, d] = PR[i]

    for i in range(len(keywords_list_all)):
        t_list = []
        for j, w in enumerate(para.weight_list):
            PR_all_result[i] = PR_all_result[i] + w * PR_all[i][j]
            t_list.append(PR_all[i][j])
        # print keywords_list_all[i], t_list

    return keywords_list_all, PR_all_result


if __name__ == '__main__':
    starttime = datetime.datetime.now()
    print 'starttime=', starttime
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "b:e:", ["begin_date=", "end_date="])
    except getopt.GetoptError:
        print "FAILED: input arguments error"
        sys.exit()

    begin_date = None
    end_date = None

    for name, value in opts:
        if name in ("--begin_date", "-b"):
            if value is None or value[0:1] == '-':
                print "FAILED: input arguments -b error"
                sys.exit()
            date_begin = int(value)
        if name in ("--end_date", "-e"):
            if value is None or value[0:1] == '-':
                print "FAILED: input arguments -e error"
                sys.exit()
            date_end = int(value)

    date_begin_1 = int((datetime.datetime.strptime(str(date_begin), '%Y%m%d') + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
    date_end_1 = int((datetime.datetime.strptime(str(date_end), '%Y%m%d') + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
    result = execute(date_begin_1, date_end_1)
    # result = execute(20180920, 20181024)
    # result = execute(20181026, 20181027)
    print result

    endtime = datetime.datetime.now()
    interval = endtime - starttime
    print '\ntime_consuming:', str(interval)

