# AnalyzeTweets
Use Python to analyze tweets stored in Postgresql database in JSON format. Run LDA, Hashtag and TextRazor analysis. 

#Tweet Collection
I have used the library Tweetset available at: https://github.com/janezkranjc/tweetset.
It uses Python to manage the collection of tweets and stores them into Postgresql.

#Postgresql
The code expects tables to have a column named "data" containing the JSON formatted tweet information coming from Twitter feed.

Several tables are created in order to store the processed and filtered data. Most of the SQL queries have been designed for the specific data I had for my university project and will need significant changes.

#Python
I was using Python version 2.6 for my code with multiple dependent libraries such as:
* Gensim
* NLTK
* psycopg2

#Latent Dirichlet Allocation (LDA)
LDA was used by grouping tweets by hashtag and running LDA to extract hidden topics (or groups of words representing a topic).

#TextRazor
TextRazor is a great and free way to extract topics, entities or other natural language products you need from text. In this implementation I also used grouped tweets by hashtag to create batches of 200KB files that are processed by TextRazor to extract the top entities.

#Tweets Cleaning
Some tweet cleaning is done to remove stop-words, extra white space, user mentions and non alphanumeric characters.
