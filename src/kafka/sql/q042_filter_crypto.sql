drop stream if exists q042_filter_crypto;

create or replace stream q042_filter_crypto
with
    ( kafka_topic='twitterdata'
    , value_format='json'
    )
as
    select *
    from q03_tweets_clean
    where tweet_group='crypto'
;