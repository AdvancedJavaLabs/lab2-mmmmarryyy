package worker;

import broker.MessageBroker;
import java.util.ArrayList;
import java.util.List;

public class WorkerManager {
    private final List<Worker> workers;

    public WorkerManager(MessageBroker broker, int workerCount) {
        this.workers = new ArrayList<>();

        for (int i = 0; i < workerCount; i++) {
            workers.add(new Worker(broker, 1));
        }
    }

    public void startAll(String placeholder, int topN) throws Exception {
        for (Worker worker : workers) {
            worker.start(placeholder, topN);
        }
        System.out.println("[WorkerManager] Started " + workers.size() + " workers");
    }

    public void stopAll() {
        for (Worker worker : workers) {
            worker.stop();
        }
    }
}