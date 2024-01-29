package org.cardinal.source;

import net.jacobpeterson.alpaca.AlpacaAPI;
import net.jacobpeterson.alpaca.model.endpoint.marketdata.common.realtime.enums.MarketDataMessageType;
import net.jacobpeterson.alpaca.model.endpoint.marketdata.stock.realtime.trade.StockTradeMessage;
import net.jacobpeterson.alpaca.websocket.marketdata.MarketDataListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A Flink SourceFunction for streaming stock trade messages from Alpaca's market data API.
 * This function connects to the Alpaca WebSocket and subscribes to trade messages for specified stock symbols.
 */
public abstract class AlpacaSourceFunction implements SourceFunction<StockTradeMessage> {

    private volatile boolean isRunning = true;
    private transient AlpacaAPI alpacaAPI;

    /**
     * Connects to Alpaca's WebSocket API and listens for stock trade messages.
     * Received messages are emitted to the Flink pipeline.
     *
     * @param ctx The SourceContext to which the StockTradeMessages are emitted.
     */
    @Override
    public void run(SourceContext<StockTradeMessage> ctx) {
        try {
            LinkedBlockingQueue<StockTradeMessage> messageQueue = new LinkedBlockingQueue<>();
            String apiKey = System.getenv("ALPACA_API_KEY");
            String apiSecret = System.getenv("ALPACA_API_SECRET");
            alpacaAPI = new AlpacaAPI(apiKey, apiSecret);

            MarketDataListener streamingListener = (messageType, message) -> {
                if (message instanceof StockTradeMessage) {
                    System.out.println(message);
                    StockTradeMessage quoteMessage = (StockTradeMessage) message;
                    ctx.collect(quoteMessage);
                }
            };
            alpacaAPI.stockMarketDataStreaming().setListener(streamingListener);

            alpacaAPI.stockMarketDataStreaming().subscribeToControl(
                    MarketDataMessageType.SUCCESS,
                    MarketDataMessageType.SUBSCRIPTION,
                    MarketDataMessageType.ERROR);
            alpacaAPI.stockMarketDataStreaming().connect();


            alpacaAPI.stockMarketDataStreaming().waitForAuthorization(5, TimeUnit.SECONDS);
            if (!alpacaAPI.stockMarketDataStreaming().isValid()) {
                System.out.println("Websocket not valid!");
                return;
            }

            alpacaAPI.stockMarketDataStreaming().subscribe(
                    null,
                    Arrays.asList("AAPL", "GOOG"),
                    null);

            while (isRunning) {
                StockTradeMessage message = messageQueue.take();
                System.out.println(message);
                ctx.collect(message);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Signals this source to stop streaming data.
     * This method is called when the job is cancelled.
     */
    @Override
    public void cancel() {
        isRunning = false;
        if (alpacaAPI != null) {
            try {
                alpacaAPI.streaming().disconnect();
            } catch (Exception e) {
                // Handle disconnection errors
            }
        }
    }

    abstract AlpacaAPI createAlpacaAPI();
}
