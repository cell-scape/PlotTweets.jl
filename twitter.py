#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging

from kafka import KafkaProducer, KafkaConsumer
import tweepy

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(message)s',
    level=logging.INFO,
    datefmt='%m/%d/%Y %I:%M:%S %p',
    encoding="utf-8",
)

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAOEDigEAAAAAjhry0xwzXKEzyUc6ZwNvr%2BnDw5Y%3D4rWidi5tZ8cJW6Bi8IMqP1E4dwNVXDl7Hjd2fj3qKamPfu8JSm"
KAFKA_HOST = "localhost"
KAFKA_PORT = "9092"
TOPIC_NAME = "tweets"

RULES = [
    tweepy.StreamRule(value="(#trump OR #Trump OR #DonaldTrump OR #Trump2020 OR #Trump2024)", tag="tweets about Trump"),
    tweepy.StreamRule(value="(#biden OR #Biden OR #JoeBiden", tag="tweets about Biden"),
    tweepy.StreamRule(value="(#democrat OR #Democrat OR #democrats OR #Democrats", tag="Tweets about democrats"),
    tweepy.StreamRule(value="(#republican OR #Republican OR #republicans OR #Republicans", tag="Tweets about republicans")
]


class StreamCollector(tweepy.StreamingClient):
    def __init__(self, bearer_token, producer, topic, wait_on_rate_limit=True):
        super().__init__(bearer_token=bearer_token, wait_on_rate_limit=wait_on_rate_limit)
        self.producer = producer
        self.topic = topic

    def on_connect(self):
        logging.info("Connected")

    def on_data(self, raw_data):
        try:
            logging.info(f"Tweet Data: {raw_data}")
            self.producer.send(self.topic, value=raw_data)
        except BaseException as e:
            logging.error(f"Error: {e}")
        return True

    def on_errors(self, status):
        logging.error(status)
        return True

    def start_stream(self):
        self.filter(
            tweet_fields=[
                "lang"
            ],
        )
#

def main():
    producer = KafkaProducer(bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}")
    consumer = KafkaConsumer(TOPIC_NAME,
        bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        fetch_max_bytes=512,
        max_poll_records=100,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    twitter_stream = StreamCollector(BEARER_TOKEN, producer, TOPIC_NAME)
    twitter_stream.start_stream()


if __name__ == "__main__":
    main()
