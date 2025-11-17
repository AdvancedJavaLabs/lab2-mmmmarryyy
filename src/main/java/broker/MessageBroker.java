package broker;

import model.ResultMessage;
import model.TaskMessage;

import java.util.function.Consumer;

public interface MessageBroker {
    void publishTask(TaskMessage taskMessage) throws Exception;
    void subscribeTasks(Consumer<TaskMessage> handler);
    void publishResult(ResultMessage resultMessage) throws Exception;
    void subscribeResults(Consumer<ResultMessage> handler);
    void close();
}
