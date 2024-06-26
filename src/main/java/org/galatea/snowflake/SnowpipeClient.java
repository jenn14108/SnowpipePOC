package org.galatea.snowflake;

import net.snowflake.ingest.streaming.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.galatea.kafka.Trade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class SnowpipeClient {

    private static Logger logger = LoggerFactory.getLogger(SnowpipeClient.class);

    String tableName = "TRADES";

    Properties properties = SnowflakeConfig.getSnowflakeConnectionProperties();

    final SnowflakeStreamingIngestClient client;

    final SnowflakeStreamingIngestChannel channel;

    boolean hasPipedDataToSnowflake = false;

    Set<Trade> trades = new HashSet<>();

    // 1 mb
   // int dataSize = 1_000_000;

    // 2 mb
   // int dataSize = 2_000_000;

    // 4 mb
    //int dataSize = 4_000_000;

    // 8 mb
    //int dataSize = 8_000_000;

    // 16 mb
   // int dataSize = 16_000_000;

    // 32 mb
    // int dataSize = 32_000_000;

    // 64 mb
   // int dataSize = 64_000_000;

    // 128 mb
    //int dataSize = 128_000_000;

    // 258 mb
    int dataSize = 256_000_000;

    int numMessages = 0;

    public SnowpipeClient() {
        client = SnowflakeStreamingIngestClientFactory.builder("CLIENT_NAME").setProperties(properties).build();
        OpenChannelRequest request = OpenChannelRequest.builder("CHANNEL" + UUID.randomUUID())
                .setDBName(properties.get("database").toString())
                .setSchemaName(properties.get("schema").toString())
                .setTableName(tableName)
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                .build();
        channel = client.openChannel(request);
    }

    public void pipeDataToSnowflake(Trade trade) throws Exception {

        Iterable<Map<String, Object>> rows = createRequestsOrCacheTrade(trade);
        if (rows == null) return;

        // convert numMessages to a string offsetToken before starting timer
        String offsetToken = String.valueOf(this.numMessages);

        long startTime = System.nanoTime();

        InsertValidationResponse response = channel.insertRows(rows, offsetToken);

        while (!offsetToken.equals(channel.getLatestCommittedOffsetToken())){
            Thread.sleep(100);
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        long durationInMs = TimeUnit.NANOSECONDS.toMillis(duration);
        logger.info("Time taken so far: {}ms", durationInMs);

        logger.info("offsetToken:{}", channel.getLatestCommittedOffsetToken());

        if (response.hasErrors()) {
            logger.error("Exception occurred when inserting data into Snowflake table.", response.getInsertErrors().get(0).getException());
        } else {
            this.hasPipedDataToSnowflake = true;
        }
    }

    public boolean hasPipedDataToSnowflake(){
        return this.hasPipedDataToSnowflake;
    }

    public void resetPipedDataToSnowflakeBoolean(){
        this.hasPipedDataToSnowflake = false;
    }

    private Iterable<Map<String, Object>> createRequestsOrCacheTrade(Trade newTrade) {

        trades.add(newTrade);

        int objectSize = getObjectSize(trades);

        if (objectSize < dataSize) {
            return null;
        }

        logger.info("data size: {}", objectSize);

        Set<Map<String, Object>> rows = new HashSet<>();

        for (Trade trade : trades) {
            Map<String, Object> rowData = new HashMap<>();
            rowData.put("TRADEID", trade.getTradeId());
            rowData.put("ACCOUNT", trade.getAccount());
            rowData.put("SECURITYID", trade.getSecurityId());
            rowData.put("QUANTITY", trade.getQuantity());
            rows.add(rowData);
        }

        getObjectSize(trades);
        this.numMessages += trades.size();

        trades.clear();
        return rows;
    }

    public int getObjectSize(Set<Trade> trades) {

        int bytes = 0;

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);

            for (Trade trade : trades) {
                oos.writeObject(trade);
                bytes += baos.size();
            }

        } catch (IOException exception) {
            logger.error("There was an error computing the object size.");
        }
        return bytes;
    }
}
