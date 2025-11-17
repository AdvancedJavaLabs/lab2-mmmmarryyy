package model;

import java.util.List;
import java.util.Map;

public class AggregatedResult {
    public long totalWordCount;
    public Map<String,Long> globalTopWords;
    public double averageSentiment;
    public List<String> combinedAnonymized;
    public List<String> allSortedSentences;

    public AggregatedResult() {}
}
