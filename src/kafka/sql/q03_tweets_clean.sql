drop stream if exists q03_tweets_clean;

create or replace stream q03_tweets_clean
with
    ( kafka_topic='twitterdata'
    , value_format='json'
    )
as
    select
        timestamptostring(stringtotimestamp(created_at, 'EEE MMM dd HH:mm:ss +0000 yyyy'), 'yyyy-MM-dd HH:mm:ss') tweet_created_datetime
        , timestamptostring(stringtotimestamp(created_at, 'EEE MMM dd HH:mm:ss +0000 yyyy'), 'yyyy-MM-dd') tweet_created_date
        , timestamptostring(stringtotimestamp(created_at, 'EEE MMM dd HH:mm:ss +0000 yyyy'), 'EEE') tweet_created_day
        , timestamptostring(stringtotimestamp(created_at, 'EEE MMM dd HH:mm:ss +0000 yyyy'), 'HH:mm:ss') tweet_created_time
        , timestamptostring(stringtotimestamp(created_at, 'EEE MMM dd HH:mm:ss +0000 yyyy'), 'HH')+':00' tweet_created_hour
        , timestamptostring(stringtotimestamp(created_at, 'EEE MMM dd HH:mm:ss +0000 yyyy'), 'yyyy-MM-dd HH')+':00' tweet_created_datehour
        , id tweet_id
        , tweet_text
        , user->id user_id
        , user->followers_count user_followers_count
        , user->friends_count user_friends_count
        , user->created_at user_created_at
        , retweet_count
        , favorite_count
        , quote_count
        , reply_count
        , is_quote_status is_quote
        , in_reply_to_user_id
        , case when is_retweet_check is null then false else is_retweet_check end is_retweet
        , case
            when instr(tweet_text, 'cryptocurrency')>0 then 'crypto'
            when instr(tweet_text, 'crypto')>0 then 'crypto'
            when instr(tweet_text, 'binance')>0 then 'crypto'
            when instr(tweet_text, 'coinbase')>0 then 'crypto'
            when instr(tweet_text, 'coinmarketcap')>0 then 'crypto'
            when instr(tweet_text, 'musk')>0 then 'crypto'
            when instr(tweet_text, 'memecoin')>0 then 'crypto'
            when instr(tweet_text, 'shitcoin')>0 then 'crypto'
            when instr(tweet_text, 'moon')>0 then 'crypto'
            when instr(tweet_text, 'hodl')>0 then 'crypto'
            when instr(tweet_text, 'fud')>0 then 'crypto'
            when instr(tweet_text, 'bitcoin')>0 then 'bitcoin'
            when instr(tweet_text, 'btc')>0 then 'bitcoin'
            when instr(tweet_text, 'ethereum')>0 then 'ethereum'
            when instr(tweet_text, 'eth')>0 then 'ethereum'
            when instr(tweet_text, 'ether')>0 then 'ethereum'
            when instr(tweet_text, 'gwei')>0 then 'ethereum'
            when instr(tweet_text, 'vitalik buterin')>0 then 'ethereum'
            when instr(tweet_text, 'gavin wood')>0 then 'ethereum'
            when instr(tweet_text, 'erc20')>0 then 'ethereum'
            when instr(tweet_text, 'dogecoin')>0 then 'dogecoin'
            when instr(tweet_text, 'doge')>0 then 'dogecoin'
            when instr(tweet_text, 'billy markus')>0 then 'dogecoin'
            when instr(tweet_text, 'jackson palmer')>0 then 'dogecoin'
            when instr(tweet_text, 'pancakeswap')>0 then 'pancakeswap'
            when instr(tweet_text, 'cake')>0 then 'pancakeswap'
            when instr(tweet_text, 'swap')>0 then 'pancakeswap'
            else null
        end tweet_group
    from q02_tweets_check_retweet
;