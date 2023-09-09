drop table if exists q043_agg_byday_byhour;

create or replace table q043_agg_byday_byhour
with
    ( kafka_topic='twitterdata'
    , value_format='json'
    )
as
    select tweet_created_datehour
        , sum(1) num_tweets
    from q03_tweets_clean
    window tumbling (size 1 day)
    group by tweet_created_datehour
;