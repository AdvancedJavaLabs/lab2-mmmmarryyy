package model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class ResultMessage {
    public int taskId;
    public long wordCount;
    public Map<String, Long> topWords;
    public long positiveCount;
    public long negativeCount;
    public String anonymizedText;
    public List<String> sortedSentences;

    @JsonCreator
    public ResultMessage(
            @JsonProperty("taskId") int taskId,
            @JsonProperty("wordCount") long wordCount,
            @JsonProperty("topWords") Map<String, Long> topWords,
            @JsonProperty("positiveCount") long positiveCount,
            @JsonProperty("negativeCount") long negativeCount,
            @JsonProperty("anonymizedText") String anonymizedText,
            @JsonProperty("sentences") List<String> sentences
    ) {
        this.taskId = taskId;
        this.wordCount = wordCount;
        this.topWords = topWords;
        this.positiveCount = positiveCount;
        this.negativeCount = negativeCount;
        this.anonymizedText = anonymizedText;
        this.sortedSentences = sentences;
    }
}
