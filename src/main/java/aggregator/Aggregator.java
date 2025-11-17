package aggregator;

import broker.MessageBroker;
import model.AggregatedResult;
import model.ResultMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class Aggregator {
    private final MessageBroker broker;
    private final LongAdder totalWords = new LongAdder();
    private final LongAdder sumPositive = new LongAdder();
    private final LongAdder sumNegative = new LongAdder();
    private final AtomicLong resultCount = new AtomicLong(0);

    private final ConcurrentHashMap<String, LongAdder> globalCounts = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> anonymizedPieces = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> allSentences = new ConcurrentLinkedQueue<>();

    private final int topN;

    public Aggregator(MessageBroker broker, int topN) {
        this.broker = broker;
        this.topN = topN;
    }

    public void start(int expectedNumberOfResults, Consumer<AggregatedResult> onComplete) throws Exception {
        broker.subscribeResults(result -> {
            merge(result);

            long currentNumberOfResults = resultCount.incrementAndGet();

            if (currentNumberOfResults >= expectedNumberOfResults) {
                System.out.println("[Aggregator] currentNumberOfResults = " + currentNumberOfResults + " expectedNumberOfResults = " + expectedNumberOfResults);

                AggregatedResult aggregated = new AggregatedResult();

                aggregated.totalWordCount = totalWords.longValue();
                aggregated.globalTopWords = getTopN(topN);
                aggregated.combinedAnonymized = new ArrayList<>(anonymizedPieces);

                long positive = sumPositive.sum();
                long negative = sumNegative.sum();
                aggregated.averageSentiment = (positive + negative) == 0 ? 0.0 : (positive - negative) / (double)(positive + negative);

                List<String> sorted = new ArrayList<>(allSentences);
                sorted.sort(Comparator.comparingInt(String::length)); // TODO: rewrite on merge in merge sort
                aggregated.allSortedSentences = sorted;

                try {
                    onComplete.accept(aggregated);
                } catch (Exception e) {
                }
            }
        });
    }

    private void merge(ResultMessage result) {
        totalWords.add(result.wordCount);
        sumPositive.add(result.positiveCount);
        sumNegative.add(result.negativeCount);

        result.topWords.forEach((k,v) -> globalCounts.computeIfAbsent(k, kk -> new LongAdder()).add(v));
        anonymizedPieces.add(result.anonymizedText);

        allSentences.addAll(result.sortedSentences);
    }

    private Map<String,Long> getTopN(int n) {
        Comparator<LongAdder> comparator = Comparator.comparingLong(LongAdder::sum).reversed();

        return globalCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(comparator))
                .limit(n)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().sum(), (a, b) -> a, HashMap::new));
    }
}
