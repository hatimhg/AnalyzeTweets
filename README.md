# AnalyzeTweets
Use Python to analyze tweets stored in Postgresql database in JSON format. Run LDA, Hashtag and TextRazor analysis. 

#Postgresql
The code expects tables to have a column named "data" containing the JSON formatted tweet information coming from Twitter feed.

Several tables are created in order to store the processed and filtered data. Most of the SQL queries have been designed for the specific data I had for my university project and will need significant changes.

#Python
I was using Python version 2.6 for my code with multiple dependent libraries such as:
* Gensim
* NLTK
* psycopg2

