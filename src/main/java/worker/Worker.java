package worker;

import broker.MessageBroker;
import model.ResultMessage;
import model.TaskMessage;
import util.TextProcessor;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class Worker {
    private final MessageBroker broker;
    private final ExecutorService pool;

    public Worker(MessageBroker broker, int parallelism) {
        this.broker = broker;
        this.pool = Executors.newFixedThreadPool(parallelism);
    }

    public void start(String placeholder, int topN) throws Exception {
        Consumer<TaskMessage> handler = task -> {
            pool.submit(() -> process(task, placeholder, topN));
        };
        broker.subscribeTasks(handler);
    }

    private void process(TaskMessage task, String placeholder, int topN) {
        try {
            long wordCount = TextProcessor.wordCount(task.textChunk);
            Map<String,Long> topWords = TextProcessor.topCounts(task.textChunk);

            long[] pn = TextProcessor.lexiconSentiment(task.textChunk);
            long positive = pn[0];
            long negative = pn[1];

            var anonimazied = TextProcessor.replaceNames(task.textChunk, placeholder);
            var sorted = TextProcessor.sortSentencesByLength(task.textChunk);

            ResultMessage res = new ResultMessage(task.id, wordCount, topWords, positive, negative, anonimazied, sorted);
            broker.publishResult(res);
        } catch (Exception e) {
        }
    }

    public void stop() {
        pool.shutdownNow();
    }
}
