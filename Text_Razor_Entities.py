# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 00:31:29 2015

@author: HatimHG
"""
import nltk
import re
import sys
import psycopg2
import time
from HTMLParser import HTMLParser
import textrazor

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

stoplist = set(nltk.corpus.stopwords.words("english"))

reload(sys)
sys.setdefaultencoding('utf8')
big_regex = re.compile(r'\b%s\b' % r'\b|\b'.join(map(re.escape, stoplist)))

def iter_docs(cursor, stoplist):
    entities = []
    cursor2 = dbconn.cursor()
    client = textrazor.TextRazor("6fac8cf468a86d442ac486f34bea9d3781f0480214ef0cd2bbce2d30",extractors=["entities","topics"])
    client.set_language_override("eng")
    text = u''
    
    for row in cursor.fetchall():
        print 'Hashtag: ', row[0]
        if(row[0] == None):
            cursor2.execute('SELECT tweet_text FROM nyc_collection_hashtag WHERE hashtag IS NULL')
        else:
            cursor2.execute('SELECT tweet_text FROM nyc_collection_hashtag WHERE hashtag LIKE %s',[row[0],])
        results = cursor2.fetchall()
        print 'Nnumber of Tweets: ', len(results)
        
        #Group tweets from a single hashtag into one big string.
        for result in results:
            temp_text=u' '+unicode(result[0],"ISO-8859-1")
            #Remove URLs
            Clean_String = re.sub(ur'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', u'  ', temp_text, flags=re.UNICODE)
            #Remove Mentions
            Clean_String = re.sub(ur'(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9_]+)', u'  ', Clean_String, flags=re.UNICODE)
            #Decode HTML charachters
            Clean_String = HTMLParser().unescape(Clean_String)
            #Remove HTML Entities
            Clean_String = strip_tags(Clean_String)
            #Delete non-alphanumeric characters and keep also underscores
            Clean_String = re.sub(ur'[\W]+', u' ', Clean_String, flags=re.UNICODE)
            #Delete words from stoplist
            #Clean_String = big_regex.sub(' ', Clean_String) #keep stoplist for TextRazor to help with semantics.
            #Remove multiple spaces
            Clean_String = re.sub(' +',' ',Clean_String)
            if sys.getsizeof(text+u' '+Clean_String) < (200*1000):
                #separating the tweets with periods might help improve the semantic results                
                text+=u' .'+Clean_String
            else:
                response = client.analyze(text.encode('utf-8'))
                for entity in response.entities():
                    entities.append({"Name":entity.id, "Score":entity.relevance_score, "Conf":entity.confidence_score})
                
                text=Clean_String
                #break
        
        response = client.analyze(text.encode('utf-8'))
        for entity in response.entities():
            entities.append({"Name":entity.id, "Score":entity.relevance_score, "Conf":entity.confidence_score})

        '''
        for entity in response.entities():
            print entity.id, entity.relevance_score, entity.confidence_score, entity.freebase_types
        
        for topic in response.topics():
            print topic.label, topic.score
        '''
        print "==================================================="
        
        #time.sleep(5)
    
    print 'Number of entities: ', len(entities)
    entities.sort(key=lambda k: k["Conf"]*k["Score"], reverse=True)
    printed = 0
    f = open("/Users/HatimHG/Desktop/GGS787/Gensim/Results/TextRazor_entities.txt","w")
    seen = []
    for entity in entities:
        if(entity["Name"] not in seen and entity["Score"]>0.3):
            f.write(entity["Name"].decode('utf-8'))
            f.write("; ")
            f.write(str(entity["Score"]))
            f.write("; ")
            f.write(str(entity["Conf"]))
            f.write("\n")
            seen.append(entity["Name"])
            printed+=1
            if(printed == 100):
                break
    cursor2.close()
    f.close()
    print 'Number of entities saved: ', printed

cursor.execute("""
SELECT hashtag, count(*) as total FROM nyc_collection_hashtag
GROUP BY hashtag ORDER BY total DESC LIMIT 50
""")

iter_docs(cursor,stoplist)
cursor.close()
dbconn.close()
