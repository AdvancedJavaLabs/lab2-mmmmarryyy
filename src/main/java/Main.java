import aggregator.Aggregator;
import broker.InMemoryBroker;
import broker.KafkaBroker;
import broker.MessageBroker;
import broker.RabbitMqBroker;
import config.AppConfig;
import producer.ProducerSplitter;
import serial.SerialRunner;
import sink.ResultSink;
import worker.Worker;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) throws Exception {
        AppConfig.Mode mode = AppConfig.Mode.SERIAL;
        AppConfig.BrokerType brokerType = AppConfig.BrokerType.INMEMORY;
        int parallelism = 4;
        int topN = 20;
        String inputPath = "input.txt";
        String outputPath = "out.json";
        AppConfig.ChunkBy chunkBy = AppConfig.ChunkBy.PARAGRAPHS;
        int chunkSize = 100;
        String placeholder = "<NAME>";

        for (int i = 0; i < args.length; i++) {
            String parameter = args[i];
            String argument = args[++i];

            switch (parameter) {
                case "--mode":
                    mode = AppConfig.Mode.valueOf(argument.toUpperCase());
                    break;
                case "--broker":
                    brokerType = AppConfig.BrokerType.valueOf(argument.toUpperCase());
                    break;
                case "--parallelism":
                    parallelism = Integer.parseInt(argument);
                    break;
                case "--topN":
                    topN = Integer.parseInt(argument);
                    break;
                case "--input":
                    inputPath = argument;
                    break;
                case "--out":
                    outputPath = argument;
                    break;
                case "--chunkBy":
                    chunkBy = AppConfig.ChunkBy.valueOf(argument.toUpperCase());
                    break;
                case "--chunkSize":
                    chunkSize = Integer.parseInt(argument);
                    break;
                case "--placeholder":
                    placeholder = argument;
                    break;
            }
        }

        if (brokerType == AppConfig.BrokerType.KAFKA) {
            throw new RuntimeException("Unsupported broker");
        }

        AppConfig config = new AppConfig(mode, brokerType, parallelism, topN, inputPath, outputPath, chunkBy, chunkSize, placeholder);

        Instant start = Instant.now();

        if (config.mode == AppConfig.Mode.SERIAL) {
            SerialRunner runner = new SerialRunner(config);
            runner.run(placeholder);
            System.out.println("[Main] Serial finished");
        } else if (config.mode == AppConfig.Mode.PARALLEL){
            MessageBroker broker = switch (config.broker) {
                case RABBITMQ -> new RabbitMqBroker("localhost", 5672, "guest", "guest");
                case KAFKA -> new KafkaBroker("localhost:9092", "group");
                default -> new InMemoryBroker();
            };

            ProducerSplitter producer = new ProducerSplitter(broker);

            int numberOfTasks = producer.splitAndPublish(Path.of(inputPath), config.chunkBy, config.chunkSize);

            CountDownLatch done = new CountDownLatch(1);
            Aggregator aggregator = new Aggregator(broker, config.topN);
            String finalOut = outputPath;

            aggregator.start(numberOfTasks, aggregatedResult -> {
                try {
                    new ResultSink().write(aggregatedResult, Path.of(finalOut));
                } catch (Exception e) {
                }

                done.countDown();
            });

            Worker worker = new Worker(broker, config.parallelism);
            worker.start(config.placeholder, config.topN);

            done.await();

            worker.stop();
            broker.close();
        }

        Instant end = Instant.now();
        System.out.println("[Main] Total time ms: " + Duration.between(start, end).toMillis());
    }
}
