function read_topic(kafka_host, kafka_port, kafka_topic; max_count=MAX_COUNT, timeout_ms=1000)
    c = KafkaConsumer("$(kafka_host):$(kafka_port)", "kafka")
    parlist = [(kafka_topic, 0)]
    subscribe(c, parlist)
    messages = []
    count = 0
    while count < max_count
        msg = poll(String, String, c, timeout_ms)
        if !isnothing(msg)
            push!(messages, JSON.Parser.parse(msg.payload))
            count += 1
        end
    end
    return messages
end

function build_dataframe(messages; rules=RULES, old_df=nothing)
    df = DataFrame(messages)
    for col in values(rules)
        df[!, col] = zeros(Bool, nrow(df))
    end
    
    for row in eachrow(df)
        for i in row[ID]
            row[rules[i]] = true
        end
    end
    isnothing(old_df) && return df
    return vcat(old_df, df)
end

