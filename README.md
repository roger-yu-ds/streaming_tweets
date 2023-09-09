# MDSI_BDE_AUT21_AT3

*MDSI - Big Data Engineering - Autumn 2021 - Assessment Task 03*

## Topics
### General
- cryptocurrency
- crypto
- binance
- coinbase
- coinmarketcap
- musk
- memecoin
- shitcoin
- moon
- hodl
- fud

### Bitcoin
- bitcoin
- btc
- ~~satoshi nakamoto~~

### Ethereum
- ethereum
- eth
- ether
- gwei
- vitalik buterin
- gavin wood
- erc20

### Dogecoin
- dogecoin
- doge
- billy markus
- jackson palmer

### PancakeSwap
- pancakeswap
- cake
- swap




---

## Repo Setup

Main Command:

```
> cookiecutter --output-dir "C:\Users\chris\OneDrive\02 - Education\07 - MDSI\10 - BDE\07 - GitHub Repo" https://github.com/drivendata/cookiecutter-data-science
```

Options:

```
> You've downloaded C:\Users\chris\.cookiecutters\cookiecutter-data-science before. Is it okay to delete and re-download it? [yes]: yes
> project_name [project_name]: BDE_AT3
> repo_name [adsi_at1]: MDSI_BDE_AUT21_AT3
> author_name [Your name (or your organization/company/team)]: chrimaho
> description [A short description of teh project.]: MDSI - Big Data Engineering - Autumn 2021 - Assessment Task 03
> Select open_source_license:
1 - MIT
2 - BSD-3-Clause
3 - No license file
Choose from 1, 2, 3 [1]: 1
> s3_bucket [[OPTIONAL] your-bucket-for-syncing-data (do not include 's3://')]: 
> aws_profile [default]: 
> Select python_interpreter:
1 - python3
2 - python
Choose from 1, 2 [1]: 1
```

Then, once it loads, run:

```
> cd MDSI_BDE_AUT21_AT2
> git init
> git add .
> git commit -m "Initial commit"
> git branch -M main
> git remote add origin https://github.com/chrimaho/MDSI_BDE_AUT21_AT3.git
> git push -u origin main
```

(Check [here](https://docs.github.com/en/github/importing-your-projects-to-github/adding-an-existing-project-to-github-using-the-command-line) or [here](https://stackoverflow.com/questions/54523848/github-setup-repository#answer-54524070) for details on `git init` process)

Or... Just run this file:
<pre>
<a href="./src/setup/make_repo.bat">./src/setup/make_repo.bat</a>
</pre>

---

## Set Up Environment

Three steps.

1. Get Docker file
   ```
   wget https://raw.githubusercontent.com/bde-uts/bde/main/bde_lab_8/Dockerfile https://raw.githubusercontent.com/bde-uts/bde/main/bde_lab_8/docker-compose.yml
   ```
   Or using `PowerShell`:
   ```
   Invoke-WebRequest https://raw.githubusercontent.com/bde-uts/bde/main/bde_lab_8/Dockerfile -OutFile "./Dockerfile"
   Invoke-WebRequest https://raw.githubusercontent.com/bde-uts/bde/main/bde_lab_8/docker-compose.yml -OutFile "./docker-compose.yml"
   ```

2. Initialise `pipenv`
   ```
   pipenv install numpy pandas ipykernel validators
   ```

3. Initialise `docker`
   ```
   docker-compose up -d
   ```

Or... Just run this file:
<pre>
<a href="./init.bat">./init.bat</a>
</pre>

---

## Dev Process

1. Overview:
   1. Jupyter Notebooks are amazing; but they're not the best for use with sourcecode management...
2. Process:
   1. If you're defining anything, *anything*, define it in a module in the [`./src/`](./src/) setup.
   2. That includes any function definitions, or classes, or anything.
   3. Then, import that module in to the notebooks using the `import` statements.
      1. If you're having issues with the import process, check out the first little section that I wrote in [`./notebooks/QuickTests.ipynb`](./notebooks/QuickTests.ipynb). It will help you.
3. Secrets:
   1. We'll be using API's to access live data. For this, we'll need some keys.
   2. Secrets and Credentials and Keys should NEVER EVER be pushed to the Repo.
   3. For this reason, we will create a `secrets.py` file.
   4. This way, we can all have separate and standalone Keys, and our code will still work with no issues.
   5. Follow the instructions on the file [`./src/secrets/secrets_template.py`](./src/secrets/secrets_template.py) for how to do this.

---

## Useful References

- Twitter
  - [Data dictionary: Standard v1.1](https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/overview)
- Overall
  - [PySpark Twitter Streaming+Kafka](https://sites.google.com/a/ku.th/big-data/pyspart)
  - [Working with Streaming Twitter Data Using Kafka](https://www.bmc.com/blogs/working-streaming-twitter-data-using-kafka/)
  - [Using Kafka to optimize data flow of your Twitter Stream](https://towardsdatascience.com/using-kafka-to-optimize-data-flow-of-your-twitter-stream-90523d25f3e8)
  - [Streaming Covid19 tweets using Kafka and Twitter API](https://elkhayati.me/kafka-python-twitter/)
  - [kafka-twitter-producer.py](https://github.com/JasonSanchez/spark-streaming-twitter-kafka/blob/master/kafka-twitter-producer.py)
  - [Easy to Play with Twitter Data Using Spark Structured Streaming](https://ch-nabarun.medium.com/easy-to-play-with-twitter-data-using-spark-structured-streaming-76fe86f1f81c)
  - [Apache Spark Streaming Tutorial: Identifying Trending Twitter Hashtags](https://www.toptal.com/apache/apache-spark-streaming-twitter)
- Kafka
  - [confluent_kafka API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration)
  - Kafka Producer [Configuration properties](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) or [Configuration properties](https://docs.confluent.io/5.5.1/clients/librdkafka/md_CONFIGURATION.html)
  - [My Python/Java/Spring/Go/Whatever Client Won’t Connect to My Apache Kafka Cluster in Docker/AWS/My Brother’s Laptop. Please Help!](https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/)
  - [python_kafka_test_client.py](https://github.com/rmoff/kafka-listeners/blob/master/python/python_kafka_test_client.py)
  - [Working with nested JSON](https://kafka-tutorials.confluent.io/working-with-nested-json/ksql.html)
  - [4 Incredible ksqlDB Techniques (#2 Will Make You Cry)](https://www.confluent.io/blog/ksqldb-techniques-that-make-stream-processing-easier-than-ever/)
  - ksqlDB [Scalar functions](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/scalar-functions/)

---

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>