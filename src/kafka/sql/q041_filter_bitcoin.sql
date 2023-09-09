drop stream if exists q041_filter_bitcoin;

create or replace stream q041_filter_bitcoin
with
    ( kafka_topic='twitterdata'
    , value_format='json'
    )
as
    select *
    from q03_tweets_clean
    where tweet_group='bitcoin'
;