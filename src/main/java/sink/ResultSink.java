package sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.AggregatedResult;

import java.nio.file.Files;
import java.nio.file.Path;

public class ResultSink {
    private final ObjectMapper mapper = new ObjectMapper();

    public void write(AggregatedResult aggregatedResult, Path out) throws Exception {
        var json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(aggregatedResult);
        Files.writeString(out, json);
    }
}

