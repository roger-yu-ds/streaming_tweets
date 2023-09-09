drop stream if exists q01_tweets_raw;

create or replace stream q01_tweets_raw
    ( created_at varchar
    , id bigint
    , user struct
        < id bigint
        , followers_count bigint
        , friends_count bigint
        , created_at varchar
        >
    , truncated boolean
    , text varchar
    , extended_tweet struct
        < full_text varchar
        >
    , retweet_count bigint
    , favorite_count bigint
    , quote_count bigint
    , reply_count bigint
    , in_reply_to_user_id varchar
    , is_quote_status boolean
    , retweeted_status struct
        < id varchar
        >
    )
with
    ( kafka_topic='twitterdata'
    , value_format='json'
    )
;