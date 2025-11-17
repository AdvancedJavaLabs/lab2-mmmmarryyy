package broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.ResultMessage;
import model.TaskMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;


public class KafkaBroker implements MessageBroker {
    private final KafkaProducer<String, byte[]> producer;
    private final KafkaConsumer<String, byte[]> taskConsumer;
    private final KafkaConsumer<String, byte[]> resultConsumer;
    private final ObjectMapper mapper = new ObjectMapper();

    private final String TASK_TOPIC = "tasks";
    private final String RESULT_TOPIC = "results";

    private volatile Consumer<TaskMessage> taskHandler;
    private volatile Consumer<ResultMessage> resultHandler;

    private final ExecutorService taskExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService resultExecutor = Executors.newSingleThreadExecutor();

    private volatile boolean running = true;

    public KafkaBroker(String bootstrapServers, String groupId) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

//        try (AdminClient adminClient = AdminClient.create(producerProperties)) {
//            // Delete the topic
//            DeleteTopicsResult deleteResult = adminClient.deleteTopics(List.of(TASK_TOPIC, RESULT_TOPIC));
//            deleteResult.all().get(); // Wait for deletion to complete
//            System.out.println("Topics deleted successfully.");
//
//            // Optional: Recreate the topic if you need it again
//            short replicationFactor = 1; // Adjust as needed
//            int numPartitions = 1;      // Adjust as needed
//            NewTopic newTopic = new NewTopic(TASK_TOPIC, numPartitions, replicationFactor);
//            NewTopic newTopic2 = new NewTopic(RESULT_TOPIC, numPartitions, replicationFactor);
//            try {
//                adminClient.createTopics(List.of(newTopic, newTopic2)).all().get();
//                System.out.println("Topic recreated successfully.");
//            } catch (ExecutionException e) {
//                if (e.getCause() instanceof TopicExistsException) {
//                    System.out.println("Topic already exists, no need to recreate. " + e.getMessage());
//                } else {
//                    throw e;
//                }
//            }
//        } catch (ExecutionException e) {
//            System.out.println("");
////            throw new RuntimeException(e);
//        } catch (InterruptedException e) {
//            System.out.println("");
////            throw new RuntimeException(e);
//        }

        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
//        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ровно 1 доставка
//        producerProperties.put(ProducerConfig.ACKS_CONFIG, "0");
        this.producer = new KafkaProducer<>(producerProperties);

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.taskConsumer = new KafkaConsumer<>(consumerProperties);
        this.resultConsumer = new KafkaConsumer<>(consumerProperties);

        taskConsumer.subscribe(List.of(TASK_TOPIC));
        resultConsumer.subscribe(List.of(RESULT_TOPIC));

        taskExecutor.submit(() -> {
            try {
                while (running && taskHandler == null) {
                }

                while (running) {
                    ConsumerRecords<String, byte[]> records = taskConsumer.poll(Duration.ofMillis(200));

                    records.forEach(record -> {
                        try {
                            TaskMessage taskMessage = mapper.readValue(record.value(), TaskMessage.class);
                            taskHandler.accept(taskMessage);
                        } catch (Exception e) {
                        }
                    });
                }
            } finally {
                taskConsumer.close();
            }
        });

        resultExecutor.submit(() -> {
            try{
                while (running && resultHandler == null) {
                }

                while (running) {
                    ConsumerRecords<String, byte[]> records = resultConsumer.poll(Duration.ofMillis(200));

                    records.forEach(record -> {
                        try {
                            ResultMessage resultMessage = mapper.readValue(record.value(), ResultMessage.class);
                            resultHandler.accept(resultMessage);
                        } catch (Exception e) {
                        }
                    });
                }
            } finally {
                resultConsumer.close();
            }
        });
    }

    @Override
    public void publishTask(TaskMessage taskMessage) throws Exception {
        byte[] data = mapper.writeValueAsBytes(taskMessage);
        producer.send(new ProducerRecord<>(TASK_TOPIC, taskMessage.id, data));
    }

    @Override
    public void subscribeTasks(Consumer<TaskMessage> handler) {
        this.taskHandler = handler;
    }

    @Override
    public void publishResult(ResultMessage resultMessage) throws Exception {
        byte[] data = mapper.writeValueAsBytes(resultMessage);
        producer.send(new ProducerRecord<>(RESULT_TOPIC, resultMessage.taskId, data));
    }

    @Override
    public void subscribeResults(Consumer<ResultMessage> handler) {
        this.resultHandler = handler;
    }

    @Override
    public void close() {
        running = false;

        try {
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            e.printStackTrace();
        }

        taskExecutor.shutdownNow();
        resultExecutor.shutdownNow();
    }
}
