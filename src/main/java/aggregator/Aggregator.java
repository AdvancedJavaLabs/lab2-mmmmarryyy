package aggregator;

import broker.MessageBroker;
import model.AggregatedResult;
import model.ResultMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private final List<String> orderedAnonymizedPieces;
    private final List<List<String>> allSortedSentencesLists;
    private final Set<Integer> processedTaskIds = ConcurrentHashMap.newKeySet();

    private final int topN;
    private final int expectedNumberOfResults;

    public Aggregator(MessageBroker broker, int topN, int expectedNumberOfResults) {
        this.broker = broker;
        this.topN = topN;
        this.expectedNumberOfResults = expectedNumberOfResults;

        this.orderedAnonymizedPieces = Collections.synchronizedList(Arrays.asList(new String[expectedNumberOfResults]));
        this.allSortedSentencesLists = Collections.synchronizedList(Arrays.asList(new ArrayList[expectedNumberOfResults]));
    }

    public void start(Consumer<AggregatedResult> onComplete) {
        broker.subscribeResults(result -> {
            if (!processedTaskIds.add(result.taskId)) {
                System.out.println("[Aggregator] SOMETHING WENT WRONG: taskId уже был обработан: " + result.taskId);
                return;
            }

            merge(result);

            long currentNumberOfResults = resultCount.incrementAndGet();

            if (currentNumberOfResults > expectedNumberOfResults) {
                System.out.println("[Aggregator] SOMETHING WENT WRONG; currentNumberOfResults = " + currentNumberOfResults + " expectedNumberOfResults = " + expectedNumberOfResults);
            }

            if (currentNumberOfResults == expectedNumberOfResults) {
                System.out.println("[Aggregator] currentNumberOfResults = " + currentNumberOfResults + " expectedNumberOfResults = " + expectedNumberOfResults);

                AggregatedResult aggregated = new AggregatedResult();

                aggregated.totalWordCount = totalWords.longValue();
                aggregated.globalTopWords = getTopN(topN);

                aggregated.combinedAnonymized = flattenAnonymizedText();

                long positive = sumPositive.sum();
                long negative = sumNegative.sum();
                aggregated.averageSentiment = (positive + negative) == 0 ? 0.0 : (positive - negative) / (double)(positive + negative);

                aggregated.allSortedSentences = mergeAllSortedSentences();

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

        int taskId = result.taskId;
        orderedAnonymizedPieces.set(taskId, result.anonymizedText);

        allSortedSentencesLists.set(taskId, result.sortedSentences);
    }

    private List<String> flattenAnonymizedText() {
        List<String> result = new ArrayList<>();

        for (String piece : orderedAnonymizedPieces) {
            if (piece != null) {
                result.add(piece);
            }
        }

        return result;
    }

    private List<String> mergeAllSortedSentences() {
        return kWayMerge(allSortedSentencesLists);
    }

    private List<String> kWayMerge(List<List<String>> lists) {
        List<String> result = new ArrayList<>();
        PriorityQueue<QueueNode> pq = new PriorityQueue<>(
                Comparator.comparingInt((QueueNode node) -> node.sentence.length())
        );

        for (int i = 0; i < lists.size(); i++) {
            List<String> list = lists.get(i);

            if (list != null && !list.isEmpty()) {
                pq.offer(new QueueNode(list.getFirst(), i, 0));
            }
        }

        while (!pq.isEmpty()) {
            QueueNode node = pq.poll();
            result.add(node.sentence);

            int nextIndex = node.index + 1;
            List<String> list = lists.get(node.listIndex);

            if (nextIndex < list.size()) {
                pq.offer(new QueueNode(list.get(nextIndex), node.listIndex, nextIndex));
            }
        }

        return result;
    }

    private record QueueNode(String sentence, int listIndex, int index) {
    }

    private Map<String,Long> getTopN(int n) {
        Comparator<LongAdder> comparator = Comparator.comparingLong(LongAdder::sum).reversed();

        return globalCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(comparator))
                .limit(n)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().sum(), (a, b) -> a, HashMap::new));
    }
}
