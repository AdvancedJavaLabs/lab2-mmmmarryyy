package broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import model.ResultMessage;
import model.TaskMessage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class RabbitMqBrokerMultipleConsumers implements MessageBroker {
    private final ConnectionFactory connectionFactory;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Connection connection;

    private final String TASK_QUEUE = "tasks";
    private final String RESULT_QUEUE = "results";

    public RabbitMqBrokerMultipleConsumers(String host, int port, String user, String password) throws Exception {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        connectionFactory.setUsername(user);
        connectionFactory.setPassword(password);
        connection = connectionFactory.newConnection();
    }

    @Override
    public void publishTask(TaskMessage taskMessage) throws Exception {
        try (Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE, true, false, false, null);
            byte[] data = mapper.writeValueAsBytes(taskMessage);
            channel.basicPublish("", TASK_QUEUE, null, data);
        }
    }

    @Override
    public void subscribeTasks(Consumer<TaskMessage> handler) throws Exception {
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE, true, false, false, null);
        channel.basicQos(1); // 1 сообщение за раз

        DeliverCallback handleDelivery = (consumerTag, delivery) -> {
            try {
                byte[] body = delivery.getBody();
                TaskMessage taskMessage = mapper.readValue(body, TaskMessage.class);
                handler.accept(taskMessage);

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                System.err.println("[RabbitMqBrokerMultipleConsumers] nack для сообщения: " + e.getMessage());
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        };

        channel.basicConsume(TASK_QUEUE, false, handleDelivery, consumerTag -> System.out.println("Consumer was cancelled: " + consumerTag));
        System.out.println("[RabbitMqBrokerMultipleConsumers] Новая подписка на очередь задач");
    }

    @Override
    public void publishResult(ResultMessage resultMessage) throws Exception {
        try (Channel channel = connection.createChannel()) {
            channel.queueDeclare(RESULT_QUEUE, true, false, false, null);
            byte[] data = mapper.writeValueAsBytes(resultMessage);
            channel.basicPublish("", RESULT_QUEUE, null, data);
        }
    }

    @Override
    public void subscribeResults(Consumer<ResultMessage> handler) throws Exception {
        Channel channel = connection.createChannel();

        channel.queueDeclare(RESULT_QUEUE, true, false, false, null);

        DeliverCallback handleDelivery = (consumerTag, delivery) -> {
            try {
                byte[] body = delivery.getBody();
                ResultMessage resultMessage = mapper.readValue(body, ResultMessage.class);
                handler.accept(resultMessage);
            } catch (Exception e) {
            }
        };

        channel.basicConsume(RESULT_QUEUE, true, handleDelivery, consumerTag -> System.out.println("Consumer was cancelled: " + consumerTag));
        System.out.println("[RabbitMqBrokerMultipleConsumers] Новая подписка на очередь результатов");
    }

    @Override
    public void close() {
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
