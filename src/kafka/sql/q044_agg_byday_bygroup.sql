drop table if exists q044_agg_byday_bygroup;

create or replace table q044_agg_byday_bygroup
with
    ( kafka_topic='twitterdata'
    , value_format='json'
    )
as
    select tweet_group
        , sum(1) num_tweets
    from q03_tweets_clean
    window tumbling (size 1 day)
    group by tweet_group
;