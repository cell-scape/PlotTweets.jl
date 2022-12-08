from kafka import KafkaClient, KafkaConsumer
import json
import pandas as pd
from bokeh.io import curdoc
from bokeh.plotting import figure
from bokeh.client import push_session

MAX_MSGS = 1000
WINDOW_SIZE = 500

RULES = {
    '1584339387256639488': "Donald Trump",
    '1584349209683439616': "2020 Election",
    '1584349209683439618': "Joe Biden",
    '1584372465630846976': "Trump 2024",
    '1584372465630846977': "Biden 2024",
    '1600736022488403968': "2022 Midterm",
    '1600736022488403971': "2022 Special Election",
    '1600736022488403969': 'Democrats',
    '1600736022488403970': 'Republicans',
    '1600736022488403972': 'Raphael Warnock',
    '1600736022488403973': 'Herschel Walker',
}


def update(consumer, df, ):
    for message in consumer.get_messages(count=MAX_MSGS):
        result = json.loads(message.message.value)


def main():
    client = KafkaClient(bootstrap_server="localhost:9092")
    consumer = KafkaConsumer("processed_tweets")
    session = push_session(curdoc())

    df = pd.DataFrame(columns=['lang', 'text', 'filtered_text', 'subjectivity', 'polarity', 'sentiment', 'Donald Trump', '2020 Election', 'Joe Biden', 'Trump 2024', 'Biden 2024', '2022 Midterm', '2022 Special Election', 'Democrats', 'Republicans' 'Raphael Warnock', 'Herschel Walker'])
    plot = Plot(
        title="Streaming Twitter Data",
        width=500,
        height=500,
        min_border=0,
        toolbar_location=None,
    )
    glyph = VBar()

if __name__ == "__main__":
    main()