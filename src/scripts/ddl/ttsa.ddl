CREATE EXTERNAL TABLE IF NOT EXISTS ttsa.tweets_raw(
  tweet_id bigint,
  t_user string,
  userhandle string,
  tweet string,
  created_time timestamp,
  location string,
  country string,
  hashtags string,
  source string,
  lang string,
  retweetcount bigint,
  favoritecount bigint,
  mentions string, 
  longitude string, 
  latitude string, 
  --fullText string, 
  timeZone string,
  totalcurrenttweetsentiment bigint,
  normalizecurrenttweetsentiment bigint, 
  currenttweetsentiment string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
LOCATION "/data/tweets/twitterdata/";


CREATE TABLE IF NOT EXISTS ttsa.tweets (
  tweet_id bigint,
  t_user string,
  userhandle string,
  tweet string,
  created_time timestamp,
  location string,
  country string,
  hashtags string,
  source string,
  lang string,
  retweetcount bigint,
  favoritecount bigint,
  mentions string, 
  longitude string, 
  latitude string, 
  --fullText string, 
  timeZone string,
  totalcurrenttweetsentiment bigint,
  normalizecurrenttweetsentiment bigint, 
  currenttweetsentiment string
  )
COMMENT 'Data about tweets from twitter API - ORC format'
CLUSTERED BY (country) INTO 5 BUCKETS 
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY 't'
STORED AS ORC tblproperties ('transactional'='true');

alter table ttsa.tweets SET TBLPROPERTIES ('transactional'='true');

create EXTERNAL TABLE IF NOT EXISTS ttsa.sample_tweets (
  createddate string,
  geolocation string,
  tweetmessage string,
  `user` struct <
    geoenabled: boolean,
    id: int,
    name: string,
    screenname: string,
    userlocation: string>
)ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/tweets/sampletwitterdata/';

select tweetmessage from sample_tweets where `user`.screenname='Aimee_Cottle';
