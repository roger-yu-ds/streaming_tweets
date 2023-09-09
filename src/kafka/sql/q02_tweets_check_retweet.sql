drop stream if exists q02_tweets_check_retweet;

create or replace stream q02_tweets_check_retweet
with
    ( kafka_topic='twitterdata'
    , value_format='json'
    )
as
    select *
        , case when len(retweeted_status->id)>0 then true else false end is_retweet_check
        , case
            when truncated=true
                then extended_tweet->full_text
            else text
        end tweet_text
    from q01_tweets_raw
;