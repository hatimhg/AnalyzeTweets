# -*- coding: utf-8 -*-
"""
Created on Thu Aug 13 00:31:29 2015

@author: HatimHG
"""
import nltk
import gensim
import re
import sys
import psycopg2
from HTMLParser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

dbconn = psycopg2.connect(dbname='tweetset', user='postgres', password='postgres')
cursor = dbconn.cursor()

#Was used before to remove unwanted characters
#delchars = ''.join(c for c in map(chr, range(256)) if not (c.isalnum() and c == ' ' and c == '_'))
stoplist = set(nltk.corpus.stopwords.words("english"))

reload(sys)
sys.setdefaultencoding('utf8')


def iter_docs(cursor, stoplist):        
    for row in cursor.fetchall():
        text = ''
        cursor2 = dbconn.cursor()
        if(row[0] == None):
            cursor2.execute('SELECT tweet_text FROM nyc_collection_hashtag WHERE hashtag IS NULL')
        else:
            cursor2.execute('SELECT tweet_text FROM nyc_collection_hashtag WHERE hashtag LIKE %s',[row[0],])
        
        results = cursor2.fetchall()
        for result in results:
            text+=u' '+unicode(result[0],"ISO-8859-1")
        cursor2.close()
        
        #Remove URLs   
        Clean_String = re.sub(ur'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', u'  ', unicode(text), flags=re.UNICODE)
        #Remove Mentions
        Clean_String = re.sub(ur'(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9_]+)', u'  ', unicode(Clean_String), flags=re.UNICODE)
        #Decode HTML charachters
        Clean_String = HTMLParser().unescape(Clean_String)
        #Remove HTML Entities
        Clean_String = strip_tags(Clean_String)
        #Delete non-alphanumeric characters and keep also underscores
        Clean_String = re.sub(ur'[\W]+', u'  ', unicode(Clean_String))
        
        #print Clean_String

        yield (x for x in 
            gensim.utils.tokenize(Clean_String, lowercase=True, deacc=True, 
                                  errors="ignore")
            if x not in stoplist)

class MyCorpus(object):

    def __init__(self, cursor, stoplist):
        self.cursor = cursor
        self.stoplist = stoplist
        self.dictionary = gensim.corpora.Dictionary(iter_docs(cursor, stoplist))
        
    def __iter__(self):
        for tokens in iter_docs(self.cursor, self.stoplist):
            yield self.dictionary.doc2bow(tokens)

cursor.execute("""
SELECT hashtag, count(*) as total FROM nyc_collection_hashtag
GROUP BY hashtag ORDER BY total DESC LIMIT 200
""")

NUM_TOPICS = 10
corpus = MyCorpus(cursor, stoplist)
# Project to LDA space
#Check Hoffman Blei Bach paper for best setting for decay variable in table 1, pg. 7.
id2word_temp = corpus.dictionary
# ignore words that appear in less than 20 documents or more than 10% documents
id2word_temp.filter_extremes(no_below=5, no_above=0.1)
lda = gensim.models.LdaModel(corpus, id2word=id2word_temp, num_topics=NUM_TOPICS,iterations=500, passes=4)

totals = 0

for i in range(0,NUM_TOPICS):
    topic = lda.show_topic(i, topn=5)
    #print "==========================================="    
    #print "Topic #"+str(i)
    #print "==========================================="
    for word in topic:
        print str(word[1])#+", ("+str(word[0])+")"
        totals+=word[0]
    #print "==========================================="    
    #print "Total Probability: "+str(totals)
    totals=0
    #print "==========================================="

cursor.close()
dbconn.close()