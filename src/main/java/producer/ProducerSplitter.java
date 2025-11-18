package producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import broker.MessageBroker;
import config.AppConfig.ChunkBy;
import model.TaskMessage;
import util.TextSplitter;

public class ProducerSplitter {
    private final MessageBroker broker;

    public ProducerSplitter(MessageBroker broker) {
        this.broker = broker;
    }

    public int splitAndPublish(Path inputFile, ChunkBy chunkBy, int chunkSize) throws Exception {
        int chunkCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            Iterator<String> chunkIterator = TextSplitter.splitStream(reader, chunkBy, chunkSize);

            while (chunkIterator.hasNext()) {
                String chunk = chunkIterator.next();
                TaskMessage message = new TaskMessage(chunkCount, chunk);

                broker.publishTask(message);

                chunkCount++;
            }
        } catch (IOException e) {
        }

        System.out.println("[ProducerSplitter] generate chunks of length = " + chunkCount + " for broker = " + broker.getClass());

        return chunkCount;
    }
}
