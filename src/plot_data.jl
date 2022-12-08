function plot_data(sent, subj)
    sentiment = plot(bar(sent, x= :sentiment, y = :count))
    subjectivity = plot(bar(subj, x=:subjectivity, y = :count))
    return sentiment, subjectivity
end