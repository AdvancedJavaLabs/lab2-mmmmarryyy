package broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import model.ResultMessage;
import model.TaskMessage;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class RabbitMqBroker implements MessageBroker {
    private final ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;
    private final ObjectMapper mapper = new ObjectMapper();

    private final String TASK_QUEUE = "tasks";
    private final String RESULT_QUEUE = "results";

    private volatile Consumer<TaskMessage> taskHandler;
    private volatile Consumer<ResultMessage> resultHandler;

    private volatile boolean running = true;

    public RabbitMqBroker(String host, int port, String user, String password) throws IOException, TimeoutException {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        connectionFactory.setUsername(user);
        connectionFactory.setPassword(password);
        connect();
    }

    private void connect() throws IOException, TimeoutException {
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();

        boolean durable = true;
        channel.queueDeclare(
                TASK_QUEUE,
                durable,
                false, // эксклюзивность
                false, // автоудаление
                null // дополнительные аргументы
        );
        channel.queueDeclare(RESULT_QUEUE, durable, false, false, null);

        channel.basicConsume(
                TASK_QUEUE,
                false,  // false == manual acknowledgment
                this::handleTaskDelivery,
                consumerTag -> System.out.println("Consumer was cancelled: " + consumerTag)
        );
        channel.basicConsume(
                RESULT_QUEUE,
                true,
                this::handleResultDelivery,
                consumerTag -> System.out.println("Consumer was cancelled: " + consumerTag)
        );
    }

    private void handleTaskDelivery(String consumerTag, Delivery delivery) {
        while (running && taskHandler == null) {
        }

        try {
            byte[] body = delivery.getBody();
            TaskMessage taskMessage = mapper.readValue(body, TaskMessage.class);
            taskHandler.accept(taskMessage);

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
            try {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            } catch (IOException ex) {
            }
        }
    }

    private void handleResultDelivery(String consumerTag, Delivery delivery) {
        while (running && resultHandler == null) {
        }

        try {
            byte[] body = delivery.getBody();
            ResultMessage resultMessage = mapper.readValue(body, ResultMessage.class);
            resultHandler.accept(resultMessage);
        } catch (IOException e) {
        }
    }

    @Override
    public void publishTask(TaskMessage taskMessage) throws Exception {
        byte[] data = mapper.writeValueAsBytes(taskMessage);
        channel.basicPublish("", TASK_QUEUE, null, data);
    }

    @Override
    public void subscribeTasks(Consumer<TaskMessage> handler) {
        this.taskHandler = handler;
    }

    @Override
    public void publishResult(ResultMessage resultMessage) throws Exception {
        byte[] data = mapper.writeValueAsBytes(resultMessage);
        channel.basicPublish("", RESULT_QUEUE, null, data);
    }

    @Override
    public void subscribeResults(Consumer<ResultMessage> handler) {
        this.resultHandler = handler;
    }

    @Override
    public void close() {
        running = false;

        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
