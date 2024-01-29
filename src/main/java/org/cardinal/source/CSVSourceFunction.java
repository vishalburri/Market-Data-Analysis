package org.cardinal.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.cardinal.model.Trade;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * A Flink SourceFunction for reading trade data from a CSV file.
 * Each line in the CSV file represents a trade, which is parsed and emitted as a Trade object.
 */
public class CSVSourceFunction implements SourceFunction<Trade> {

    private final String pathToFile;
    private volatile boolean isRunning = true;

    /**
     * Creates a new CSVSourceFunction to read trade data from the specified file path.
     *
     * @param pathToFile The path to the CSV file containing trade data.
     */
    public CSVSourceFunction(String pathToFile) {
        this.pathToFile = pathToFile;
    }

    /**
     * Reads trade data from the CSV file line by line, parses each line into a Trade object,
     * and emits it to the Flink pipeline.
     *
     * @param ctx The SourceContext to which the parsed Trade objects are emitted.
     * @throws Exception If an error occurs during file reading or parsing.
     */
    @Override
    public void run(SourceContext<Trade> ctx) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(pathToFile))) {
            String line;
            while (isRunning && (line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                // Assuming the first line is the header and skipping it
                if (fields[0].equals("Symbol")) {
                    continue;
                }
                Trade trade = new Trade(
                        fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2]),
                        Integer.parseInt(fields[3]),
                        fields[4],
                        LocalDateTime.parse(fields[5], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                );
                ctx.collect(trade);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading from CSV", e);
        }
    }

    /**
     * Signals this source to stop reading data.
     * This method is called when the job is cancelled.
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}

