package org.cardinal.functions;


import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class AverageAggregateTest {
    private static AverageAggregate averageAggregate;

    @BeforeAll
    public static void setUp() {
        averageAggregate = new AverageAggregate();
    }

    @Test
    public void testCreateAccumulator() {
        Tuple2<Double, Integer> accumulator = averageAggregate.createAccumulator();
        Assertions.assertEquals(0.0, accumulator.f0, 0.0);
        Assertions.assertEquals(0, (int)accumulator.f1);
    }

    @Test
    public void testAdd() {
        Tuple2<Double, Integer> accumulator = averageAggregate.createAccumulator();
        accumulator = averageAggregate.add(1.0, accumulator);
        accumulator = averageAggregate.add(2.0, accumulator);
        Assertions.assertEquals(3.0, accumulator.f0, 0.0);
        Assertions.assertEquals(2, (int)accumulator.f1);
    }

    @Test
    public void testGetResult() {
        Tuple2<Double, Integer> accumulator = averageAggregate.createAccumulator();
        accumulator = averageAggregate.add(1.0, accumulator);
        accumulator = averageAggregate.add(2.0, accumulator);
        double result = averageAggregate.getResult(accumulator);
        Assertions.assertEquals(1.5, result, 0.0);
    }

    @Test
    public void testMerge() {
        Tuple2<Double, Integer> a = new Tuple2<>(10.0, 2);
        Tuple2<Double, Integer> b = new Tuple2<>(20.0, 3);
        Tuple2<Double, Integer> merged = averageAggregate.merge(a, b);
        Assertions.assertEquals(30.0, merged.f0, 0.0);
        Assertions.assertEquals(5, (int)merged.f1);
    }

}