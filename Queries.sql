#=========================================================================================================

#For NYC, to prepare data from Raw JSON to data meeting my needs we follow these steps:
#  1- Create table containing the desired 1) fields, 2) date range and 3) Language
#  2- Create table with repeating tweets for each hashtag mentioned in tweet.

#  1- Step One
SELECT collect_tweet.data::json#>>'{id}' as tweet_id,
collect_tweet.data::json#>>'{text}' as tweet_text,
collect_tweet.data::json#>>'{user,screen_name}' as username,
to_timestamp(
    collect_tweet.data::json#>>'{created_at}', 'DY Mon DD HH24:MI:SS +0000 YYYY'
) as tweet_date,
collect_tweet.data::json#>>'{entities,hashtags}' as hashtags,
ST_FlipCoordinates(
    st_geomfromgeojson(
        collect_tweet.data::json#>>'{geo}'
                      )
) as geom
INTO nyc_collection
FROM collect_tweet
WHERE collect_tweet.data::json#>>'{geo,type}' = 'Point'
AND collect_tweet.data::json#>>'{lang}' like '%en%'
AND to_timestamp(
    collect_tweet.data::json#>>'{created_at}', 'DY Mon DD HH24:MI:SS +0000 YYYY'
) >= '2015-08-18 13:00:00'
AND to_timestamp(
    collect_tweet.data::json#>>'{created_at}', 'DY Mon DD HH24:MI:SS +0000 YYYY'
) <= '2015-08-19 04:00:00'
AND collect_tweet.collection_id = 10;


#  2- Step Two, have a copy of the tweet for each hashtag
SELECT t1.*, t2.hashtag
INTO nyc_collection_hashtag
FROM nyc_collection t1 LEFT JOIN (
    	SELECT nyc_collection.tweet_id,
    	json_array_elements_text(nyc_collection.hashtags::json)::json ->> 'text' as hashtag
    	FROM nyc_collection
) t2
ON t1.tweet_id = t2.tweet_id;

SELECT t1.*, t2.hashtag
INTO nyc_collection_hashtag
FROM nyc_collection t1 LEFT JOIN (
    	SELECT nyc_collection.tweet_id,
    	json_array_elements_text(nyc_collection.hashtags::json)::json ->> 'text' as hashtag
    	FROM nyc_collection
    	WHERE (nyc_collection.hashtags NOT ILIKE '%Hiring%' AND nyc_collection.hashtags NOT ILIKE '%CareerArc%' AND nyc_collection.hashtags NOT ILIKE '%job%' AND nyc_collection.hashtags NOT ILIKE '%Retail%' AND nyc_collection.hashtags NOT ILIKE '%Hospitality%'
    	AND nyc_collection.hashtags NOT ILIKE '%Veterans%' AND nyc_collection.hashtags NOT ILIKE '%Nursing%' AND nyc_collection.hashtags NOT ILIKE '%Sales%'
    	AND nyc_collection.hashtags NOT ILIKE '%Healthcare%' AND nyc_collection.hashtags NOT ILIKE '%IT%' AND nyc_collection.hashtags NOT ILIKE '%CustomerService%'
    	AND nyc_collection.hashtags NOT ILIKE '%BusinessMgmt%'
    	AND nyc_collection.hashtags NOT ILIKE '%Accounting%' AND nyc_collection.hashtags NOT ILIKE '%traffic%' AND nyc_collection.hashtags NOT ILIKE '%Clerical%' AND nyc_collection.hashtags NOT ILIKE '%Marketing%' AND nyc_collection.hashtags NOT ILIKE '%Finance%'
    	AND nyc_collection.hashtags NOT ILIKE '%internship%'
)
) t2
ON t1.tweet_id = t2.tweet_id
WHERE (t1.hashtags NOT ILIKE '%Hiring%' AND t1.hashtags NOT ILIKE '%CareerArc%' AND t1.hashtags NOT ILIKE '%job%' AND t1.hashtags NOT ILIKE '%Retail%' AND t1.hashtags NOT ILIKE '%Hospitality%'
    	AND t1.hashtags NOT ILIKE '%Veterans%' AND t1.hashtags NOT ILIKE '%Nursing%' AND t1.hashtags NOT ILIKE '%Sales%'
    	AND t1.hashtags NOT ILIKE '%Healthcare%' AND t1.hashtags NOT ILIKE '%IT%' AND t1.hashtags NOT ILIKE '%CustomerService%'
    	AND t1.hashtags NOT ILIKE '%BusinessMgmt%'
    	AND t1.hashtags NOT ILIKE '%Accounting%' AND t1.hashtags NOT ILIKE '%traffic%' AND t1.hashtags NOT ILIKE '%Clerical%' AND t1.hashtags NOT ILIKE '%Marketing%' AND t1.hashtags NOT ILIKE '%Finance%'
    	AND t1.hashtags NOT ILIKE '%internship%'
);

#When conducting the LDA by Hashtag only use the top 100 after removing the common top hashtags
SELECT hashtag, count(*) as total FROM nyc_collection_hashtag WHERE hashtag NOT IN (
'Hiring','CareerArc','job','Job','Jobs','hiring','Retail','Hospitality','Veterans','Nursing','Sales','Healthcare','IT','CustomerService','BusinessMgmt','Accounting','traffic','Clerical','Marketing','Finance','internship'
) OR hashtag IS NULL
GROUP BY hashtag ORDER BY total DESC LIMIT 100

#=========================================================================================================

#For USA, to prepare data from Raw JSON to data meeting my needs we follow these steps:
#  1- Create table containing the desired 1) fields, 2) date range and 3) Language
#  2- Create table with repeating tweets for each hashtag mentioned in tweet.

#  1- Step One
SELECT usa_json.data::json#>>'{_id}' as tweet_id,
usa_json.data::json#>>'{text}' as tweet_text,
usa_json.data::json#>>'{user,screen_name}' as username,
to_timestamp(
    usa_json.data::json#>>'{created_at}', 'DY Mon DD HH24:MI:SS +0000 YYYY'
) as tweet_date,
usa_json.data::json#>>'{entities,hashtags}' as hashtags,
st_geomfromgeojson(
    usa_json.data::json#>>'{coordinates}'
) as geom
INTO usa_collection
FROM usa_json
WHERE usa_json.data::json#>>'{coordinates,type}' = 'Point'
AND usa_json.data::json#>>'{lang}' = 'en'
AND to_timestamp(
    usa_json.data::json#>>'{created_at}', 'DY Mon DD HH24:MI:SS +0000 YYYY'
) >= '2015-08-18 13:00:00'
AND to_timestamp(
    usa_json.data::json#>>'{created_at}', 'DY Mon DD HH24:MI:SS +0000 YYYY'
) <= '2015-08-19 04:00:00';


#  2- Step Two, have a copy of the tweet for each hashtag
SELECT t1.*, t2.hashtag
INTO nyc_collection_hashtag
FROM nyc_collection t1 LEFT JOIN (
    	SELECT nyc_collection.tweet_id,
    	json_array_elements_text(
            nyc_collection.hashtags::json
        )::json ->> 'text' as hashtag
    	FROM nyc_collection
) t2
ON t1.tweet_id = t2.tweet_id;

SELECT t1.*, t2.hashtag
INTO usa_collection_hashtag
FROM usa_collection t1 LEFT JOIN (
    	SELECT usa_collection.tweet_id,
    	json_array_elements_text(usa_collection.hashtags::json)::json ->> 'text' as hashtag
    	FROM usa_collection
    	WHERE (usa_collection.hashtags NOT ILIKE '%Hiring%' AND usa_collection.hashtags NOT ILIKE '%CareerArc%' AND usa_collection.hashtags NOT ILIKE '%job%' AND usa_collection.hashtags NOT ILIKE '%Retail%' AND usa_collection.hashtags NOT ILIKE '%Hospitality%'
    	AND usa_collection.hashtags NOT ILIKE '%Veterans%' AND usa_collection.hashtags NOT ILIKE '%Nursing%' AND usa_collection.hashtags NOT ILIKE '%Sales%'
    	AND usa_collection.hashtags NOT ILIKE '%Healthcare%' AND usa_collection.hashtags NOT ILIKE '%IT%' AND usa_collection.hashtags NOT ILIKE '%CustomerService%'
    	AND usa_collection.hashtags NOT ILIKE '%BusinessMgmt%'
    	AND usa_collection.hashtags NOT ILIKE '%Accounting%' AND usa_collection.hashtags NOT ILIKE '%traffic%' AND usa_collection.hashtags NOT ILIKE '%Clerical%' AND usa_collection.hashtags NOT ILIKE '%Marketing%' AND usa_collection.hashtags NOT ILIKE '%Finance%'
    	AND usa_collection.hashtags NOT ILIKE '%internship%'
)
) t2
ON t1.tweet_id = t2.tweet_id
WHERE (t1.hashtags NOT ILIKE '%Hiring%' AND t1.hashtags NOT ILIKE '%CareerArc%' AND t1.hashtags NOT ILIKE '%job%' AND t1.hashtags NOT ILIKE '%Retail%' AND t1.hashtags NOT ILIKE '%Hospitality%'
    	AND t1.hashtags NOT ILIKE '%Veterans%' AND t1.hashtags NOT ILIKE '%Nursing%' AND t1.hashtags NOT ILIKE '%Sales%'
    	AND t1.hashtags NOT ILIKE '%Healthcare%' AND t1.hashtags NOT ILIKE '%IT%' AND t1.hashtags NOT ILIKE '%CustomerService%'
    	AND t1.hashtags NOT ILIKE '%BusinessMgmt%'
    	AND t1.hashtags NOT ILIKE '%Accounting%' AND t1.hashtags NOT ILIKE '%traffic%' AND t1.hashtags NOT ILIKE '%Clerical%' AND t1.hashtags NOT ILIKE '%Marketing%' AND t1.hashtags NOT ILIKE '%Finance%'
    	AND t1.hashtags NOT ILIKE '%internship%'
);

#When conducting the LDA by Hashtag only use the top 200 after removing the common top hashtags
SELECT hashtag, count(*) as total FROM usa_collection_hashtag WHERE (hashtag NOT IN (
'Retail','Hospitality','Veterans','Nursing','Sales','Healthcare','IT','CustomerService','BusinessMgmt','Accounting','traffic','Clerical','Marketing','Finance','internship'
) AND hashtag NOT ILIKE '%job%' AND hashtag NOT ILIKE '%hiring%' AND hashtag NOT ILIKE '%career%') OR hashtag IS NULL
GROUP BY hashtag ORDER BY total DESC LIMIT 200
#=========================================================================================================

# Calculations:

# 1- Top 20 hashtags
SELECT hashtag, count(*) as counts
FROM nyc_collection_hashtag
GROUP BY hashtag
ORDER BY counts DESC
LIMIT 20;

# 2- Find tweets per minute to get time pattern.
select date_trunc(
    'minute', tweet_date
) as tweet_date1, count(
    *
) as total_tweets
FROM nyc_collection
GROUP BY tweet_date1
ORDER BY tweet_date1;

# 3- Update the SRID since it is unknown
SELECT UpdateGeometrySRID(
    'nyc_collection','geom',4326
);

# 4- Create hashtag indexes using B-Tree
CREATE INDEX hashtag_search_idx ON nyc_collection_hashtag(hashtag);
CREATE INDEX hashtag_search_idx2 ON usa_collection_hashtag(hashtag);

# 5- Count tweets in timezone
SELECT timezones.tzid, count(*) FROM timezones INNER JOIN usa_collection ON st_within(usa_collection.geom,timezones.geom)
GROUP BY timezones.tzid

# 6- Create GIST indexes to speed geom searching
CREATE INDEX geom_idx2 ON usa_collection USING GIST(geom);

# 7- Count tweets in timezone
SELECT date_trunc(
    'minute', tweet_date
) as tweet_date1, count(
    *
) as total_tweets
FROM usa_collection INNER JOIN timezones ON st_within(usa_collection.geom,timezones.geom)
WHERE timezones.tzid = 'America/Los_Angeles'
GROUP BY tweet_date1
ORDER BY tweet_date1;


###########################################################################################

#I want to create a new table that contains the two areas I want to clip the data to.

#Delete tweets outside of boundaries
#NYC Cleaning
SELECT count(*) from nyc_collection, boundaries on NOT ST_Disjoint(nyc_collection.geom,boundaries.geom)
WHERE boundaries.name = 'NYC'

#USA Cleaning, Disjoint is a bad idea it took more than 1 hour and didn't finish.
DELETE FROM usa_collection WHERE usa_collection.tweet_id IN (SELECT tweet_id FROM usa_collection INNER JOIN boundaries ON ST_Disjoint(usa_collection.geom,boundaries.geom)
WHERE boundaries.name = 'USA')

SELECT usa_collection.* INTO usa_collection_clean FROM usa_collection INNER JOIN boundaries ON ST_Within(usa_collection.geom,boundaries.simple_geom)
WHERE boundaries.name = 'USA'

