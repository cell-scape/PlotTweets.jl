module PlotTweets

using CSV
using DataFrames
using DataFramesMeta
using JSON
using PlotlyJS
using RDKafka

include("constants.jl")
include("read_topic.jl")
include("plot_data.jl")

end
