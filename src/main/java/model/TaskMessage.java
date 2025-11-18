package model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskMessage {
    public int id;
    public String textChunk;

    @JsonCreator
    public TaskMessage(
            @JsonProperty("taskId") int taskId,
            @JsonProperty("chunk") String chunk
    ) {
        this.id = taskId;
        this.textChunk = chunk;
    }
}

