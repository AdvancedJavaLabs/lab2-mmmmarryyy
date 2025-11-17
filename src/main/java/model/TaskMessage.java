package model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class TaskMessage {
    public String id;
    public String textChunk;

    public static TaskMessage of(String textChunk) {
        return new TaskMessage(UUID.randomUUID().toString(), textChunk);
    }

    @JsonCreator
    public TaskMessage(
            @JsonProperty("taskId") String taskId,
            @JsonProperty("chunk") String chunk
    ) {
        this.id = taskId;
        this.textChunk = chunk;
    }
}

