package broker;

import model.ResultMessage;
import model.TaskMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;


public class InMemoryBroker implements MessageBroker {
    private final BlockingQueue<TaskMessage> taskQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<ResultMessage> resultQueue = new LinkedBlockingQueue<>();

    private volatile Consumer<TaskMessage> taskHandler;
    private volatile Consumer<ResultMessage> resultHandler;

    private final ExecutorService taskExecutor;
    private final ExecutorService resultExecutor;

    private volatile boolean running = true;

    public InMemoryBroker() {
        taskExecutor = Executors.newSingleThreadExecutor();
        resultExecutor = Executors.newSingleThreadExecutor();

        taskExecutor.execute(() -> {
            try {
                while (running && taskHandler == null) {
                }

                while (running) {
                    TaskMessage taskMessage = taskQueue.take();
                    taskHandler.accept(taskMessage);
                }
            } catch (Exception e) {
            }
        });

        resultExecutor.execute(() -> {
            try {
                while (running && resultHandler == null) {
                }

                while (running) {
                    ResultMessage resultMessage = resultQueue.take();
                    resultHandler.accept(resultMessage);
                }
            } catch (Exception e) {
            }
        });
    }

    @Override
    public void publishTask(TaskMessage taskMessage) throws Exception {
        taskQueue.put(taskMessage);
    }

    @Override
    public void subscribeTasks(Consumer<TaskMessage> handler) {
        this.taskHandler = handler;
    }

    @Override
    public void publishResult(ResultMessage resultMessage) throws Exception {
        resultQueue.put(resultMessage);
    }

    @Override
    public void subscribeResults(Consumer<ResultMessage> handler) {
        this.resultHandler = handler;
    }

    @Override
    public void close() {
        running = false;
        taskExecutor.shutdownNow();
        resultExecutor.shutdownNow();
    }
}
