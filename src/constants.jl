const RULES = Dict{String, String}(
    "1584339387256639488" => "Donald Trump",
    "1584349209683439616" => "2020 Election",
    "1584349209683439618" => "Joe Biden",
    "1584372465630846976" => "Trump 2024",
    "1584372465630846977" => "Biden 2024",
    "1600736022488403968" => "2022 Midterm",
    "1600736022488403971" => "2022 Special Election",
    "1600736022488403969" => "Democrats",
    "1600736022488403970" => "Republicans",
    "1600736022488403972" => "Raphael Warnock",
    "1600736022488403973" => "Herschel Walker",
)

const KAFKA_HOST = "localhost:9092"
const TOPIC = "processed_tweets"
const MAX_COUNT = 200

# Columns

const LANG = "lang"
const TEXT = "text"
const FILTERED = "filtered_text"
const POLARITY = "polarity"
const SENTIMENT = "sentiment"
const ID = "id"
const SUBJECTIVITY = "subjectivity"
const BIDEN = "Joe Biden"
const TRUMP = "Donald Trump"
const WARNOCK = "Raphael Warnock"
const WALKER = "Herschel Walker"
const BIDEN24 = "Biden 2024"
const TRUMP24 = "Trump 2024"
const DEMOCRATS = "Democrats"
const REPUBLICANS = "Republicans"
const ELECTION22 = "2022 Special Election"
const MIDTERM22 = "2022 Midterm"
