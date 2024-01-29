package org.cardinal.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.cardinal.model.Trade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

class CSVSourceFunctionTest {
    private SourceContext sourceContextMock;
    private String testFilePath = "test_trades.csv";

    @Captor
    private ArgumentCaptor<Trade> tradeCaptor;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        sourceContextMock = mock(SourceContext.class);

        // Create a test CSV file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(testFilePath))) {
            writer.write("Symbol,Trade ID,Price,Size,Exchange,Timestamp\n");
            writer.write("AAPL,123,150.5,10,NASDAQ,2021-01-01 10:00:00\n");
            writer.write("GOOG,124,2520.0,5,NYSE,2021-01-01 10:05:00\n");
        }
    }

    @Test
    void testCSVSourceFunction() throws Exception {
        CSVSourceFunction sourceFunction = new CSVSourceFunction(testFilePath);
        sourceFunction.run(sourceContextMock);

        // Capture the actual Trade objects passed to collect
        verify(sourceContextMock, times(2)).collect(tradeCaptor.capture());
        Trade capturedTrade1 = tradeCaptor.getAllValues().get(0);
        Trade capturedTrade2 = tradeCaptor.getAllValues().get(1);

        // Asserting the properties of the trades
        assertTrade("AAPL", 123, 150.5, 10, "NASDAQ", "2021-01-01 10:00:00", capturedTrade1);
        assertTrade("GOOG", 124, 2520.0, 5, "NYSE", "2021-01-01 10:05:00", capturedTrade2);
    }

    private void assertTrade(String symbol, long tradeId, double price, int size, String exchange, String timestampStr, Trade actual) {
        LocalDateTime timestamp = LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        assertAll(
                () -> assertEquals(symbol, actual.getSymbol()),
                () -> assertEquals(price, actual.getPrice(), 0.01),
                () -> assertEquals(timestamp, actual.getTimestamp())
        );
    }
}

