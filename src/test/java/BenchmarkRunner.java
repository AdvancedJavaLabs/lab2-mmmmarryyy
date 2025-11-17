import java.io.FileWriter;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import config.AppConfig.BrokerType;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception {
        List<String> sizes = List.of("2MB", "50MB", "100MB");
        List<BrokerType> brokers = List.of(BrokerType.RABBITMQ, BrokerType.INMEMORY);
        List<Integer> parallels = List.of(1, 2, 4, 8, 12);
        List<String> chunkByOptions = List.of("BYTES", "PARAGRAPHS", "SENTENCES");
        Map<String, List<Integer>> chunkSizeOptionsMap = Map.of(
                "2MB", List.of(1000, 5000, 10000),
                "50MB", List.of(10000, 50000),
                "100MB", List.of(50000, 100000)
        );

        String inputDir = "bench_inputs";
        String outCsv = "benchmarks/results.csv";

        try (FileWriter fileWriter = new FileWriter(outCsv, false)) {
            fileWriter.write("timestamp,mode,broker,parallelism,chunkBy,chunkSize,inputSize,timeMs\n");
        }

        for (String size: sizes) {
            List<Integer> chunkSizeOptions = chunkSizeOptionsMap.get(size);
            String file = inputDir + "/example_" + size + ".txt";

            for (String chunkBy: chunkByOptions) {
                for (int chunkSize: chunkSizeOptions) {
                    String[] serialArguments = new String[]{
                            "--mode", "serial",
                            "--input", file,
                            "--out", "bench_outputs/out_serial_" + size + ".json",
                            "--chunkBy", chunkBy,
                            "--chunkSize", String.valueOf(chunkSize),
                            "--topN", "20",
                            "--placeholder", "<NAME>"
                    };

                    long startSerial = System.nanoTime();
                    Main.main(serialArguments);
                    long elapsedSerial = (System.nanoTime() - startSerial) / 1_000_000;

                    try (FileWriter fileWriter = new FileWriter(outCsv, true)) {
                        fileWriter.write(String.join(",",
                                Instant.now().toString(),
                                "serial",
                                "-",
                                "1",
                                chunkBy,
                                String.valueOf(chunkSize),
                                size,
                                String.valueOf(elapsedSerial)) + "\n"
                        );
                    }
                }
            }

            for (BrokerType broker: brokers) {
                String brokerName = broker.name().toLowerCase();
                System.out.println("[BenchmarkRunner]: Testing broker = " + brokerName + ", size = " + size);

                for (int parallelizm: parallels) {
                    for (String chunkBy: chunkByOptions) {
                        for (int chunkSize: chunkSizeOptions) {
                            String[] runArgs = new String[]{
                                "--mode", "parallel",
                                "--broker", brokerName,
                                "--parallelism", String.valueOf(parallelizm),
                                "--input", file,
                                "--out", "bench_outputs/out_" + brokerName + "_" + size + "_p" + parallelizm + "_" + chunkBy.toLowerCase() + "_" + chunkSize + ".json",
                                "--chunkBy", chunkBy,
                                "--chunkSize", String.valueOf(chunkSize),
                                "--topN", "20",
                                "--placeholder", "<NAME>"
                            };

                            long start = System.nanoTime();
                            Main.main(runArgs);
                            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

                            try (FileWriter fileWriter = new FileWriter(outCsv, true)) {
                                fileWriter.write(String.join(",",
                                        Instant.now().toString(),
                                        "parallel",
                                        brokerName,
                                        String.valueOf(parallelizm),
                                        chunkBy,
                                        String.valueOf(chunkSize),
                                        size,
                                        String.valueOf(elapsedMs)) + "\n"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
