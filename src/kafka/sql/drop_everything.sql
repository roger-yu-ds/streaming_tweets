drop stream if exists q01_tweets_raw;
drop stream if exists q02_tweets_check_retweet;
drop stream if exists q03_tweets_clean;
drop stream if exists q041_filter_bitcoin;
drop stream if exists q042_filter_crypto;
drop table if exists q043_agg_byday_byhour;
drop table if exists q044_agg_byday_bygroup;