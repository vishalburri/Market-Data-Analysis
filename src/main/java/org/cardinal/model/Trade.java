package org.cardinal.model;

import java.time.LocalDateTime;

/**
 * Represents a trade transaction with details such as symbol, trade ID, price, size, exchange, and timestamp.
 */
public class Trade {
    private final String symbol;
    private final long tradeId;
    private final double price;
    private final int size;
    private final String exchange;
    private final LocalDateTime timestamp;

    /**
     * Constructs a new Trade object.
     *
     * @param symbol    The symbol of the traded stock or asset.
     * @param tradeId   The unique identifier of the trade.
     * @param price     The price at which the trade occurred.
     * @param size      The size of the trade.
     * @param exchange  The exchange where the trade occurred.
     * @param timestamp The timestamp of when the trade occurred.
     */
    public Trade(String symbol, long tradeId, double price, int size, String exchange, LocalDateTime timestamp) {
        this.symbol = symbol;
        this.tradeId = tradeId;
        this.price = price;
        this.size = size;
        this.exchange = exchange;
        this.timestamp = timestamp;
    }

    public String getSymbol() {
        return symbol;
    }

    public double getPrice() {
        return price;
    }
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "symbol='" + symbol + '\'' +
                ", tradeId=" + tradeId +
                ", price=" + price +
                ", size=" + size +
                ", exchange='" + exchange + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
