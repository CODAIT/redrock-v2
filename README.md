# RedRock v2

RedRock v2 is an application developed to facilitate big data analysis. It allows users to discover insights from Twitter about any term(s) they are interested in. RedRock v2 can search through terabytes of data in seconds and transforms that data into an easy-to-digest JSON output that can be used to generate a set of visualizations. This functionality is accessible via a REST-API for easy access and consumption. RedRock v2 is powered by Apache Spark.

## Overview

How to configure local environment and **RedRock v2** code to run in standalone mode

In this guide it is assumed you are using a mac, but it can easily translate to any linux distribution

## Running RedRock v2

### Download RedRock v2 code

Clone the RedRock v2 Backend code at: <https://github.com/SparkTC/tiara>

In case you can't access the repo, please contact Luciano Resende for authorization.

Configure environment variable TIARA_HOME at your shell initialization file with the path to your RedRock v2 directory. For example: at your **/home/.profile** add the line: **export TIARA_HOME=/Users/YOURUSERNAME/Projects/tiara**  


### Environment Setup

#### Hadoop

Install hadoop 2.6+

Follow this guide to configure and execute hadoop on standalone (for mac) mode: <http://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite>

Create hdfs directories that will be used by RedRock v2

```
$TIARA_HOME/dev/make-hdfs-dirs.sh
```

#### Redis

Install the latest version of redis

```
brew install redis
```

#### Apache Spark

Download **pre-built Apache Spark 1.6.1 for Hadoop 2.6 and later** and decompress it (<http://spark.apache.org/downloads.html>).

###### Configuring Apache Spark in standalone mode

1. Configure environment variable SPARK_HOME at your shell initialization file with the path to the directory where your Apache Spark is installed

    * For example: at your **/home/.profile** add the line **export SPARK_HOME=/Users/YOURUSERNAME/Spark/spark-1.6.1-bin-hadoop2.6**
2. Save file _conf/slaves.template_ as _conf/slaves_
3. Save file _conf/spark-env.sh.template_ as _conf/spark-env.sh_ and add the following lines:
    * **HADOOP_CONF_DIR**=/usr/local/Cellar/hadoop/2.7.0/libexec/etc/hadoop/
        * Hadoop home path where you can find the configuration files like hdfs-site.xml  and  core-site.xml
    * **SPARK_WORKER_DIR**=/Users/YOURUSERNAME/opt/SparkData
        * Create a local directory to save Spark logs
    * **SPARK_WORKER_CORES**=1
        * Define the amount of cores to be used for each worker instance. Keep in mind the number of logical cores your machine has.
    * **SPARK_WORKER_INSTANCES**=5
        * Define it based on how many logical cores your machine has. Keep in mind that each worker instance is going to use the amount of worker cores you defined. In this current setup, we are using 5 cores at total, which means 5 (workers) * 1 core (worker-cores)
    * **SPARK_WORKER_MEMORY**=2g
        * Define it based on how much memory RAM your machine has. Keep in mind that each worker instance is going to use the amount of worker memory you defined. In this current setup, we are allocating 10g of memory, which means 5 (workers) * 2g (worker-memory)
    * **SPARK_DRIVER_MEMORY**=4g
        * Define it based on how much memory RAM your machine has. In this current setup our total memory RAM is 16g and we have already allocated 10g for workers.


4. Save file _conf/log4j.properties.template_ as _conf/log4j.properties_ **log4j.rootCategory=WARN**. Save it as

Note: The above Apache Spark setup is considering a machine with at least:

1. 16gb of memory RAM
2. 4 cores (8 logical cores)

#### SBT plugin

Install sbt plugin. More information at <http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Mac.html>

### Starting applications

Before running **RedRock v2** you must start all the following applications:

1. **Apache Hadoop**: Use command **hstart** (in case you followed the installation instruction at the Hadoop section)
2. **Redis**: Run **redis-server** (note: you may want to use nohup for it to run in the background)
3. **Apache Spark**: Inside Apache Spark home path use command **./sbin/start-all.sh**

### Configuring RedRock v2

All the configurations for RedRock v2 are at: **TIARA_HOME/conf/tiara-app.conf.template**. Copy this file and save at the same location without the .template extension.

All the default configurations are considering that you followed all the steps above and you haven't changed any configurations for Apache Spark. In case you have a different setup, please take a look at the section **[Explaining the RedRock v2 Configuration File](#rrconfig)**

### Understanding Data Directories

The pre-process of the tweets is going to happen through Apache Spark. Apache Spark will use the following directories configured in **TIARA_HOME/conf/tiara-app.conf.template**:

1. **tiara.decahose-processor.historical.data-path**: Any data here will be processed before the streaming starts in the Decahose processor. The variable **tiara.decahose-processor.historical.enabled** controls if you process the historical data.
2. **tiara.decahose-processor.decahose-dir**: The poll Decahose actor will put Decahose data here and Apache Spark streaming in the Decahose processor will monitor this directory in order to process any new data.
3. **tiara.decahose-processor.daily-en-tweets-dir**: The Decahose processor stores all the english tweets for each day here for use by the REST API.
4. **tiara.decahose-processor.tokens-dir**: = The Decahose processor stores all the word token data for use by Apache Spark Word2Vec here
5. **tiara.word-2-vec.path-to-daily-models**: = This is where Word2Vec places models after computation

### Downloading Twitter Data

To download the Twitter data you have to have access to Bluemix. In order to get access to Bluemix, follow the instruction in the section **[Getting Access to Bluemix](#bluemix)**

You can play around with the **[IBM Insights for Twitter API](#bluemix-api)** and download a sample of tweets to use as input on RedRock v2.

#### Decahose Historical

Make sure your downloaded historical data is inside the hadoop Decahose historical folder (_/data/rawTweets_).

```
hadoop fs -put DEST_FOLDER/historical*.json.gz /data/rawTweets
```

#### Decahose Streaming

RedRock v2 Streaming uses _Apache Spark Streaming_ application, which monitors an HDFS directory for new files.

You can simulate a streaming processing by pasting a file on the streaming directory being monitored while the streaming application is running. The file should be processed on the next streaming bach.

### Running RedRock v2

RedRock v2 code is divided into 4 main applications: decahose-poll-actor, decahose-processor, word2vec-models, and rest-api.

To start all the applications at once, use the script **TIARA_HOME/bin/start-all.sh**

To stop all the applications at once, use the script **TIARA_HOME/bin/stop-all.sh**

Each application can be started and stopped individually. Use the start and stop scripts in **TIARA_HOME/bin/**

The log file for each application file will be at:

1. Actor: **TIARA_HOME/decahose-poll-actor/nohup-actor.out**
2. Decahose: **TIARA_HOME/decahose-processor/nohup-decahose.out**
3. Word2Vec: **TIARA_HOME/word2vec-models/nohup-word2vec.out**
4. Rest API: **TIARA_HOME/rest-api/nohup-restapi.out**

**Don't forget to put the files to be analysed into the HDFS directories we defined before.**

### Making requests

To send a request to the REST API just open a browser and use one of the URLs specified below.

#### Get Synonyms

Sample URL: <http://localhost:16666/tiara/getsynonyms?searchterm=%23spark&count=10>

Parameters:

1. **searchterm**: Term to search for related terms
2. **count**: The number of related terms to return


#### Response

The getsynonyms request retrieves the top X related terms to the search term with their relation and the count of each term. The response is in JSON format.

```
{
   "success":true,
   "status":0,
   "searchTerm":"#spark",
   "searchTermCount":27,
   "distance":[
      [
         "#hadoop",
         "1209152.8714422116", // relation to search term
         "72" // term count
      ],
      ... // Top X terms related to your search
   ]
}
```

#### Get Top Terms


Sample URL: <http://localhost:16666/tiara/gettopterms?count=20>

Parameters:

1. **count**: number of terms to retrieve

#### Response

The gettopterms request retrieves the top X hashtags and handles. The response is in JSON format.

```
{
   "success":true,
   "status":0,
   "hashtags":[
      {
         "term":"#whcd",
         "score":56104 // term count
      },
      ...
   ],
   "handles":[
      {
         "term":"@youtube",
         "score":66485
      },
      ...
   ]
}
```

#### Get Communities


Sample URL: <http://localhost:16666/tiara/getcommunities?searchterms=%23spark,%23apm,%23dataanalytics,%23developer,%23predictiveanalytics,%23businessintelligence,%23hadoop,%23software,%23devops,%23datamining,%23deeplearning,%23opensource,%23abdsc,%23mobility,%23paas,%23nosql,%23dataviz,%23containers,%23docker,%23microservices,%23datascience&get3d=false&top=20>

Parameters:

1. **searchterms**: Build communities from tweets with these terms
2. **get3d**: Return two or three dimensional coordinate data


#### Response

The getcommunities request retrieves a users graph and coordinate data for plotting the communities visualization. The response is in JSON format.

```
{
   "success":true,
   "status":0,
   "node":[
      "label",
      "id",
      "degree",
      "community",
      "x",
      "y",
      "z"
   ],
   "edge":[
      "id",
      "source",
      "target",
      "weight"
   ],
   "communities":{
      "nodes":[
         [
            "IBMDevOps",
            "42",
            8,
            "0",
            21.480810165405273,
            95.94647216796875,
            0
         ],
         ...
      ],
      "edges":[
         [
            "IBMDevOps",
            "41",
            "42",
            "2.0"
         ],
         ...
      ]
   }
}

```

#### Get Communities Details


Sample URL: <http://loaclhost:16666/tiara/getcommunitiesdetails?searchterms=%23spark,%23apm,%23dataanalytics,%23developer,%23predictiveanalytics,%23businessintelligence,%23hadoop,%23software,%23devops,%23datamining,%23deeplearning,%23opensource,%23abdsc,%23mobility,%23paas,%23nosql,%23dataviz,%23containers,%23docker,%23microservices,%23datascience&count=20>

Parameters:

1. **searchterm**: Terms to build the communities from
2. **count**: The number of wordcloud terms to return


#### Response

The getcommunitiesdetails request retrieves sentiment and wordcloud data for the communities based on the search terms. The wordcloud data represents the top X most used terms in a communities tweets. The response is in JSON format.

```
{
   "success":true,
   "sentiment":{
      "communityID":[
         "positive",
         "negative",
         "neutral"
      ]
   },
   "wordcloud":{
      "communityID":[
         "word",
         "count"
      ]
   },
   "communitydetails":{
      "sentiment":{
         "0":[
            6,
            3,
            132
         ],
         ... // for each community found for the given search terms
      },
      "wordcloud":{
         "0":[
            [
               "#devops",
               106
            ],
            ... // top X terms for the community
         ],
         ... // for each community found for the given search terms
      }
   }
}

```

### <a name="rrconfig"></a> Explaining the RedRock v2 Configuration File

The RedRock v2 configuration file is located at **TIARA_HOME/conf/tiara-app.conf.template**. The key root for each section is listed at the top of each table.

#### Application -- tiara.\*

| **Key**                       | **Meaning**           | **Default**  |
| ----------------------------- |-----------------------| -------------|
| app-name                      | Application name      | "TIARA"
| hadoop-default-fs             | Location of hdfs      | "hdfs://localhost:9000"
| date-time-format-to-display   | Timestamp format for raw twitter data | "yyyy-MM-dd HH:mm:ss"

#### Decahose -- tiara.decahose-processor.\*

| **Key**               | **Meaning**           | **Default**  |
| --------------------- |-----------------------| -------------|
| historical.enabled    | Process historical data before starting streaming | true
| historical.data-path  | Data path for the historical data | ${tiara.hadoop-default-fs}"/data/rawTweets/\*"
| decahose-dir          | Data path for the streaming data | ${tiara.hadoop-default-fs}"/tiara/decahose/streaming"
| daily-en-tweets-dir   | Data path for the english tweets | ${tiara.hadoop-default-fs}"/tiara/en"
| tokens-dir            | Data path for the token data | ${tiara.hadoop-default-fs}"/tiara/toks"
| debug-dir             | Data path for development debug data | ${tiara.hadoop-default-fs}"/tiara/debug"
| batch-server-listen-port | Batch server bind port | "9999"
| post-date-col-name    | Date column name      | "postedDate"
| tokens-column         | Token column name    | "toks"
| redis-tweet-entity-token-count | Redis token prefix | "ES"
| sentiment-column      | Sentiment column name | "sentiment"
| update-redis-counters | Update redis counters | true
| redis-server          | Redis host            | "localhost"
| redis-port            | Redis host port       | "6379"
| tweet-schema-json     | Tweet schema file     | "decahose-large.json"
| writing-mode-string   | Temporary file name concatination while writing | "\_WRITING_"
| spark.sql-shuffle-partitions | Number of Spark partitions | 5
| spark.UI-port         | Port to bind the Decahose UI Spark application | "4040"
| spark.executor-memory | Amount of executor memory to be used by the Decahose Spark Application | "2g"
| spark.cores-max       | Max cores to be used by Spark in the application | 4
| spark.executor-cores  | number of cores to be used on each executor | 1
| spark.streaming-batch-time | Interval in seconds to process streaming data | 60
| spark.delete-file-after-processed | Delete file after it has been processed by Spark | false

#### Decahose Poll Actor -- tiara.decahose-processor.poll-decahose-actor.\*

| **Key**               | **Meaning**           | **Default**  |
| --------------------- |-----------------------| -------------|
| timezone              | Timezone for Decahose requests | "PST"
| fileNameFormatForURL  | Twitter data path format in hdsf | "yyyy/MM/dd/yyyy_MM_dd_HH_mm"
| fileNameFormat        | Twitter data file name format | "yyyy_MM_dd_HH_mm"
| fileNameExtensionJson | Twitter data file extension | "activity.json.gz"
| fileNameExtensionGson | Twitter data file extension for data previous to June 2015 | "activity.gson.gz"
| user                  | Bluemix user          | "########"
| password              | Bluemix password      | "###########"
| hostname              | Bluemix host          | "https://cde-archive&#46;services.dal.bluemix.net/"
| startDownloadingAfter | How many seconds after the application have started will the task begin to be executed | 10
| timeInterval          | Time interval in seconds that the task will be executed | 60

#### Word2Vec -- tiara.word-2-vec.\*

| **Key**               | **Meaning**           | **Default**  |
| --------------------- |-----------------------| -------------|
| historical.enabled    | Create models for given date range before starting the streaming | false
| historical.start-date | Start of date range for computation | "2016-04-21"
| historical.end-date   | End of date range for computation | "2016-04-21"
| path-to-daily-tweets  | Data path for the token data | ${tiara.decahose-processor.tokens-dir}
| path-to-daily-models  | Data path for the models | ${tiara.hadoop-default-fs}"/tiara/models/daily"
| col-name-tweet-txt    | Token column name    | ${tiara.decahose-processor.tokens-column}
| prefix-tokens-folder-daily | Token data folder name prefix | ${tiara.decahose-processor.post-date-col-name}"="
| date-format           | Token data folder name format | "yyyy-MM-dd"
| folder-name-model     | Word2Vec model file name | "word2VecModel"
| folder-name-word-count | Word count file name | "wordCount"
| token-file-name       | New model token file name | "newModel.txt"
| use-only-hashtag-handle | Model only uses hashtags and handle tokens | true
| run-new-model-at      | Hour of the day to run new model | 1
| generate-model-timeinterval | Time interval in seconds that the task will be executed | 86400
| parameters.partition-number | Partitions parameter for Word2Vec | 6
| parameters.iterations-number | Iterations parameter for Word2Vec | 6
| parameters.min-word-count | Minimum word count parameter for Word2Vec | 10
| parameters.vector-size | Vector size parameter for Word2Vec | 100
| parameters.window-size | Window size parameter for Word2Vec | 5
| spark.sql-shuffle-partitions | Number of Spark partitions | 5
| spark.UI-port         | Port to bind the Word2Vec UI Spark application | "4041"
| spark.executor-memory | Amount of executor memory to be used by the Word2Vec Spark Application | "1g"
| spark.cores-max       | Max cores to be used by Spark in the application | 4
| spark.executor-cores  | number of cores to be used on each executor | 1

#### Rest API -- tiara.rest-api.\*

| **Key**                      | **Meaning**               | **Default**  |
| -----------------------------|---------------------------| -------------|
| bind-port                    | REST API bind port        | 16666
| bind-ip                      | REST API ip address       | "0.0.0.0"
| daily-en-tweets-dir          | Data path for the english tweets | ${tiara.decahose-processor.daily-en-tweets-dir}
| path-to-daily-models         | Data path for the models  | ${tiara.word-2-vec.path-to-daily-models}
| token-file-name              | New model token file name | ${tiara.word-2-vec.token-file-name}
| folder-name-model            | Word2Vec model file name  | ${tiara.word-2-vec.folder-name-model}
| folder-name-word-count       | Word count file name      | ${tiara.word-2-vec.folder-name-word-count}
| date-format                  | Token data folder name format | ${tiara.word-2-vec.date-format}
| sentiment-column-name        | Sentiment column name     | ${tiara.decahose-processor.sentiment-column}
| tokens-column-name           | Tokens column name        | ${tiara.decahose-processor.tokens-column}
| redis-server                 | Redis host                | ${tiara.decahose-processor.redis-server}
| redis-port                   | Redis host port           | ${tiara.decahose-processor.redis-port}
| redis-key-entity             | Redis token prefix        | ${tiara.decahose-processor.redis-tweet-entity-token-count}
| prefix-tokens-folder-daily   | Token data folder name prefix | ${tiara.word-2-vec.prefix-tokens-folder-daily}
| cache-graph-membership       | Cache community membership for future calls | true
| membership-graph-expiration  | How many seconds community membership is cached | 600
| return-only-hashtag-synonyms | getsynonyms call only returns hashtags | true
| start-scheduler-after        | How many seconds after the application have started will the scheduler begin to be executed | 0
| check-for-new-model-interval | Time interval in seconds that the actor checks for a new model | 600
| spark.sql-shuffle-partitions | Number of Spark partitions | 5
| spark.UI-port                | Port to bind the Rest API UI Spark application | "4042"
| spark.executor-memory        | Amount of executor memory to be used by the Rest API Spark Application | "1g"
| spark.cores-max              | Max cores to be used by Spark in the application | 4
| spark.executor-cores         | number of cores to be used on each executor | 1

## <a name="bluemix"></a> Getting Access to Bluemix

How to create a free trial account on IBM Bluemix and download sample tweets.

### Sign Up for IBM Bluemix
If you still don't have an Bluemix account, follow this steps to sing up for a trial one.

1. Access IBM Bluemix website on <https://console.ng.bluemix.net>

2. Click on **Get Started Free**.

   ![bluemix-home-page](https://www.evernote.com/shard/s709/sh/f8a08eb9-a246-4340-95ae-31a49fe612af/ad4602c6a05068ec/res/db1d6c2b-6cc7-4cd3-851c-d9ada2edea70/skitch.png?resizeSmall&width=832 =400x100)

3. Fill up the registration form and click on **Create Account**.

   ![bluemix-form](https://www.evernote.com/shard/s709/sh/6683326c-b47d-4737-80e9-c689e22a7d67/deb1d4135dd83c95/res/8b37ed82-cbe9-4474-bfd9-f605a46c8e89/skitch.png?resizeSmall&width=832 =400x250)

4. Check out the email you use to sign up, look for IBM Bluemix email and click on **Confirm your account**.

     ![bluemix-email](https://www.evernote.com/shard/s709/sh/17cc9c86-b7b0-42e2-8764-4a4bcbb86cbb/1552ee3a236a23d3/res/00feece8-464a-471f-b9ed-920f94663e9e/skitch.png?resizeSmall&width832 =300x100)

5. Log In to Bluemix with the credentials you just created.
6. Click on **Catalog** at the top left of your screen
7. Search for **Twitter** and click on **View More**

   ![bluemix-twitter](https://www.evernote.com/shard/s709/sh/0c47004d-d5d9-4641-a285-3b01801a9430/b6d79dae573d26b6/res/1c2d8e6c-61d8-491d-8f13-bd9340e0ff9c/skitch.png?resizeSmall&width=832 =400x200)

8. On the right side of the page fill in the service name and the credential name. They must be populated but the contents do not matter for this tutorial. Click on **Create**.

   **Notes**: Make sure the _Free Plan_ is selected.

   ![bluemix-tt-service](https://www.evernote.com/shard/s709/sh/7bc9d809-e547-477e-a013-3167258d8173/0da1737fdbed779a/res/c22136c1-2ab3-4d04-9b2d-fb2b2c92a3d8/skitch.png?resizeSmall&width=832 =400x200)

9. <a name="bluemix-credential"></a> You can find your credentials by clicking on the service at the **Dashboard** and then clicking on **Service Credentials**'

   ![bluemix-tt-credentials](https://www.evernote.com/shard/s709/sh/13f88f83-bb6d-45fd-ae29-661cb35fef5a/ffa747f0ddbbf13f/res/797c7c68-bf62-4adc-a0fe-2e429540c677/skitch.png?resizeSmall&width=832 =400x200)

### <a name="bluemix-api"></a> Accesing Bluemix Twitter API

Browse _IBM Insights for Twitter API_ documentation and try the APIs before you use them.

1. You can learn about the IBM Bluemix Twitter Service at: <https://console.ng.bluemix.net/docs/services/Twitter/index.html#twitter>

2. Try the APIs at: <https://cdeservice.mybluemix.net/rest-api>

   **Note**: Powertrack API is not available on _Free Plan_ (and not used in RedRock v2)

   ![bluemix-tt-API](https://www.evernote.com/shard/s709/sh/e818c8e5-ffc3-4d4e-a6b4-a69307683396/2780cb0f7a6f542c/res/46ab0653-c3d5-49e3-857a-876b9dc99bae/skitch.png?resizeSmall&width=832 =400x200)


---
*<sub><sup>TN: TIARA stands for Team Infrastructure, Automation and Reference Application</sup></sub>*