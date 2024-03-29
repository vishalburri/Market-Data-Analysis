package org.cardinal.functions;

import net.jacobpeterson.alpaca.model.endpoint.marketdata.stock.realtime.trade.StockTradeMessage;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class ConditionalAverageAggregateTest {
    private static ConditionalAverageAggregate aggregateFunction;

    @BeforeAll
    public static void setUp() {
        aggregateFunction = new ConditionalAverageAggregate();
    }

    @Test
    public void testCreateAccumulator() {
        Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> accumulator = aggregateFunction.createAccumulator();
        Assertions.assertTrue(accumulator.f0.isEmpty());
        Assertions.assertTrue(accumulator.f1.isEmpty());
        Assertions.assertEquals(0, (int)accumulator.f2);
    }

    @Test
    public void testAdd() {
        Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> accumulator = aggregateFunction.createAccumulator();
        StockTradeMessage trade1 = new StockTradeMessage();
        trade1.setSymbol("AAPL");
        trade1.setPrice(100.0);
        StockTradeMessage trade2 = new StockTradeMessage();
        trade2.setSymbol("AAPL");
        trade2.setPrice(110.0);

        accumulator = aggregateFunction.add(trade1, accumulator);
        accumulator = aggregateFunction.add(trade2, accumulator);

        Assertions.assertEquals(1, accumulator.f2.intValue());
        Assertions.assertEquals(2, accumulator.f1.get("AAPL").size());
        Assertions.assertEquals(110.0, accumulator.f0.get("AAPL"), 0.01);
    }

    @Test
    public void testGetResult() {
        Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> accumulator = aggregateFunction.createAccumulator();
        StockTradeMessage trade1 = new StockTradeMessage();
        trade1.setSymbol("AAPL");
        trade1.setPrice(100.0);
        StockTradeMessage trade2 = new StockTradeMessage();
        trade2.setSymbol("AAPL");
        trade2.setPrice(110.0);

        accumulator = aggregateFunction.add(trade1, accumulator);
        accumulator = aggregateFunction.add(trade2, accumulator);

        double result = aggregateFunction.getResult(accumulator);
        Assertions.assertEquals(105.0, result, 0.01);
    }

    @Test
    public void testMerge() {
        Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> a = aggregateFunction.createAccumulator();
        Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> b = aggregateFunction.createAccumulator();

        StockTradeMessage trade1 = new StockTradeMessage();
        trade1.setSymbol("AAPL");
        trade1.setPrice(100.0);
        StockTradeMessage trade2 = new StockTradeMessage();
        trade2.setSymbol("GOOG");
        trade2.setPrice(200.0);

        a = aggregateFunction.add(trade1, a);
        b = aggregateFunction.add(trade2, b);

        Tuple3<Map<String, Double>, Map<String, List<Double>>, Integer> merged = aggregateFunction.merge(a, b);

        Assertions.assertEquals(2, merged.f2.intValue());
        Assertions.assertEquals(100.0, merged.f0.get("AAPL"), 0.01);
        Assertions.assertEquals(200.0, merged.f0.get("GOOG"), 0.01);
        Assertions.assertEquals(1, merged.f1.get("AAPL").size());
        Assertions.assertEquals(1, merged.f1.get("GOOG").size());
    }

}