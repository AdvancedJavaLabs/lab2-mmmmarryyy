package config;

public class AppConfig {
    public enum Mode { SERIAL, PARALLEL }
    public enum BrokerType { INMEMORY, RABBITMQ, KAFKA }
    public enum ChunkBy { PARAGRAPHS, SENTENCES, BYTES }

    public final Mode mode;
    public final BrokerType broker;
    public int parallelism;
    public int topN;
    public String inputPath;
    public String outputPath;
    public final ChunkBy chunkBy;
    public final int chunkSize;
    public final String placeholder;

    public AppConfig(
            Mode mode,
            BrokerType broker,
            int parallelism,
            int topN,
            String inputPath,
            String outPath,
            ChunkBy chunkBy,
            int chunkSize,
            String placeholder
    ) {
        this.mode = mode;
        this.broker = broker;
        this.parallelism = parallelism;
        this.topN = topN;
        this.inputPath = inputPath;
        this.outputPath = outPath;
        this.chunkBy = chunkBy;
        this.chunkSize = chunkSize;
        this.placeholder = placeholder;
    }
}
