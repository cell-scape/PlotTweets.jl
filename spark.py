#! /usr/bin/env python3.10
# -*- coding: utf-8 -*-

import string
import re

import findspark
from pyspark.sql import functions
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StringType, StructType, StructField, FloatType, ArrayType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, decode, get_json_object, to_json

from nltk.tokenize import word_tokenize
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import subjectivity, stopwords
from nltk.sentiment import SentimentAnalyzer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.sentiment.util import *

KAFKA_HOST = "127.0.0.1"
KAFKA_PORT = "9092"
TOPIC_NAME = "tweets"
SPARK_KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"


def train_subjectivity_model(train, test):
    n = train + test
    subj_docs = [(sent, 'subj') for sent in subjectivity.sents(categories='subj')[:n]]
    obj_docs = [(sent, 'obj') for sent in subjectivity.sents(categories='obj')[:n]]
    train_subj_docs = subj_docs[:train]
    test_subj_docs = subj_docs[train:]
    train_obj_docs = obj_docs[:train]
    test_obj_docs = obj_docs[train:]
    training_docs = train_subj_docs + train_obj_docs
    test_docs = test_subj_docs + test_obj_docs
    sentiment_analyzer = SentimentAnalyzer()
    all_words_neg = sentiment_analyzer.all_words([mark_negation(doc) for doc in training_docs])
    unigram_feats = sentiment_analyzer.unigram_word_feats(all_words_neg, min_freq=4)
    sentiment_analyzer.add_feat_extractor(extract_unigram_feats, unigrams=unigram_feats)
    training_set = sentiment_analyzer.apply_features(training_docs)
    test_set = sentiment_analyzer.apply_features(test_docs)
    trainer = NaiveBayesClassifier.train
    classifier = sentiment_analyzer.train(trainer, training_set)
    for k, v in sorted(sentiment_analyzer.evaluate(test_set).items()):
        print(f"{k}: {v}")
    return sentiment_analyzer


SUBJ_ANALYZER = train_subjectivity_model(800, 200);
SID = SentimentIntensityAnalyzer()


def classify_subjectivity(tweet):
    return SUBJ_ANALYZER.classify(tweet)


def vader_polarity(tweet):
    return SID.polarity_scores(tweet)["compound"]


def sentiment(tweet):
    if tweet == 0.0:
        return "Neutral"
    if tweet > 0.0:
        return "Positive"
    if tweet < 0.0:
        return "Negative"
    return None


def all_json(lang, text, id, filtered, subj, pol, sent):
    return json.dumps({"lang": lang, "text": text, "id": id, "filtered_text": filtered, "subjectivity": subj, "polarity": pol, "sentiment": sent})


def sanitize(tweet):
    # Lowercase, Strip Whitespace
    tweet = str(tweet).lower().strip()

    # Remove Hashtags, Users
    tweet = " ".join(list(filter(lambda w: w[0] not in ('#', "@"), tweet.split())))

    # Remove Links
    tweet = " ".join(list(filter(lambda w: not ("http://" in w or "bit.ly" in w or "[link]" in w), tweet.split())))

    # Filter punctuation, numbers, stopwords
    tweet = "".join(list(filter(lambda ch: ch not in string.punctuation or ch not in string.digits, tweet)))
    # tweet = " ".join(list(filter(lambda w: w not in stopwords.words("english"), word_tokenize(tweet))))
    return tweet


def main():
    findspark.add_packages(SPARK_KAFKA_PKG)
    spark = SparkSession.builder.appName("TwitterStreamingDashboard").config("spark.jars.packages", SPARK_KAFKA_PKG).getOrCreate()
    sdf = spark.readStream.format("kafka").option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}").option("subscribe", TOPIC_NAME).option("startingOffsets", "latest").load()
    rule_schema = StructType([
        StructField("id", StringType(), True),
        StructField("tag", StringType(), True)
    ])
    data_schema = StructType([
        StructField("edit_history_tweet_ids", ArrayType(StringType()), True),
        StructField("id", StringType(), True),
        StructField("lang", StringType(), True),
        StructField("text", StringType(), True),
    ])
    schema = StructType([
        StructField("data", data_schema, True),
        StructField("matching_rules", ArrayType(rule_schema), True)
    ])
    values = sdf.select(from_json(sdf.value.cast("string"), schema).alias("tweet"))
    df = values.select("tweet.data.lang", "tweet.data.text", "tweet.matching_rules.id")
    sanitizer = udf(sanitize, StringType())
    tweets_df = df.withColumn('filtered_text', sanitizer(col('text')))
    subjective = udf(classify_subjectivity, StringType())
    polarity = udf(vader_polarity, FloatType())
    jsonify = udf(all_json, StringType())
    sent = udf(sentiment, StringType())
    subj_tweets = tweets_df.withColumn('subjectivity', subjective(col('filtered_text')))
    pol_tweets = subj_tweets.withColumn('polarity', polarity(col('filtered_text')))
    sent_tweets = pol_tweets.withColumn('sentiment', sent(col('polarity')))
    out_df = sent_tweets.withColumn('value', jsonify(col('lang'), col('text'), col('id'), col('filtered_text'), col('subjectivity'), col('polarity'), col('sentiment')))
    query = out_df.writeStream.outputMode("append").format("kafka").option('kafka.bootstrap.servers', 'localhost:9092').option("topic", "processed_tweets").option("checkpointLocation", "./checkpoint").start()

    query.awaitTermination()


if __name__ == "__main__":
    main()

