package serial;

import config.AppConfig;
import model.AggregatedResult;
import sink.ResultSink;
import util.TextProcessor;
import util.TextSplitter;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class SerialRunner {
    private final AppConfig config;

    public SerialRunner(AppConfig config) {
        this.config = config;
    }

    public AggregatedResult run(String placeholder) throws Exception {
        long totalWords = 0;
        Map<String,Long> globalTopN = new HashMap<>();
        long positiveSum = 0;
        long negativeSum = 0;
        List<String> anonymized = new ArrayList<>();
        List<String> allSentences = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Path.of(config.inputPath))) {
            Iterator<String> chunkIterator = TextSplitter.splitStream(reader, config.chunkBy, config.chunkSize);

            while (chunkIterator.hasNext()) {
                String chunk = chunkIterator.next();
                
                long wordCount = TextProcessor.wordCount(chunk);
                totalWords += wordCount;
                
                var top = TextProcessor.topNCounts(chunk, config.topN);
                top.forEach((k, v) -> globalTopN.merge(k, v, Long::sum));
                
                long[] positiveNegative = TextProcessor.lexiconSentiment(chunk);
                positiveSum += positiveNegative[0];
                negativeSum += positiveNegative[1];
                
                anonymized.add(TextProcessor.replaceNames(chunk, placeholder));
                allSentences.addAll(TextProcessor.sortSentencesByLength(chunk));
            }
        }

        double sentiment = TextProcessor.lexiconSentiment(positiveSum, negativeSum);

        AggregatedResult aggregatedResult = new AggregatedResult();
        aggregatedResult.totalWordCount = totalWords;
        aggregatedResult.globalTopWords = TopN(globalTopN, config.topN);
        aggregatedResult.averageSentiment = sentiment;
        aggregatedResult.combinedAnonymized = anonymized;
        aggregatedResult.allSortedSentences = allSentences;

        new ResultSink().write(aggregatedResult, Path.of(config.outputPath));

        return aggregatedResult;
    }

    private Map<String,Long> TopN(Map<String,Long> map, int n) {
        return map.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(n)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b)->a, HashMap::new));
    }
}
