package org.galatea;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.galatea.kafka.Trade;
import org.galatea.service.TradeService;
import org.galatea.snowflake.SnowflakeConfig;
import org.galatea.snowflake.SnowpipeClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.galatea.kafka.CommonConfig.GROUP_ID;

@SpringBootApplication
@EnableJpaRepositories("org.galatea.persistence.repository")
@EntityScan("org.galatea.kafka")
@ComponentScan(basePackages = {"org.galatea"})
public class Application {
    private static Logger logger = LoggerFactory.getLogger(Application.class);

    @Autowired
    TradeService tradeService;

    @Autowired
    SnowpipeClient snowpipeClient;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

//    public static final int numMessagesToPublish =  1_000_000_000;
   // public static final int numMessagesToPublish = 1000;

    String accountName = "tirajwk-pha86896";
    String tableName = "TRADES";

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public ApplicationRunner runner(@Qualifier("KafkaProducer") KafkaProducer<Long, Trade> producer) {
        return args -> {

            int id = 1;
            int numBatches = 1;

            while (numBatches <= 10) {
                Trade trade = new Trade(id, "NUM_MSG_1K_BATCH", "1301.TW", 311);
                snowpipeClient.pipeDataToSnowflake(trade);
                id++;

                if (snowpipeClient.hasPipedDataToSnowflake()) {
                    numBatches++;
                    snowpipeClient.resetPipedDataToSnowflakeBoolean();
                }
            }

//            while (!snowpipeClient.hasPipedDataToSnowflake()) {
//                Trade trade = new Trade(id, "NUM_MSG_1K_BATCH", "1301.TW", 311);
//                snowpipeClient.pipeDataToSnowflake(trade);
//                id++;
//            }
        };
    }


//    @Bean
//    public ApplicationRunner runner(@Qualifier("KafkaProducer") KafkaProducer<Long, Trade> producer) {
//        return args -> {
//
//            int numMessages = 1;
//            int id = 90000;
//
//            while (numMessages <= numMessagesToPublish) {
//                // create sample trade
//                Trade trade = new Trade(id, "ACCOUNT_BATCH_5_3", "1301.TW", 311);
//                //logger.info("Sending trade...");
//                producer.send(new ProducerRecord("trade_topic", 6L, trade));
//                id++;
//                numMessages++;
//                // publish a message every second
//                // Thread.sleep(1000);
//            }
//
//            producer.close();
//            logger.info("COMPLETED PUBLISHING MESSAGES");
//            // wait for 1 minute then start the listener after all messages are published
//            Thread.sleep(10000);
//            registry.getListenerContainer(GROUP_ID).start();
//        };
//    }

//    @KafkaListener(id = GROUP_ID, topics = "trade_topic", groupId = GROUP_ID)
//    public void kafkaListener(Trade trade) throws Exception {
//        logger.info("KafkaListener received trade message: {}", trade);
//        // save the trade into the h2 database
//        // tradeService.saveTrade(trade);
//        // logger.info("Trade successfully saved into h2 database");
//        // logger.info("Trades in h2 db: {}", tradeService.fetchAllTrades());
//        // now try to pipe data to snowflake
//        snowpipeClient.pipeDataToSnowflake(trade);
//    }

//    @Bean
//    public ApplicationRunner runner(KafkaTemplate<Long, Trade> template) {
//        return args -> {
//
//            int numMessages = 1;
//            int id = 90000;
//
//            while (numMessages <= numMessagesToPublish) {
//                // create sample trade
//                Trade trade = new Trade(id, "ACCOUNT_BATCH_5_3", "1301.TW", 311);
//                //logger.info("Sending trade...");
//                template.send("trade_topic", (long) id, trade);
//                id++;
//                numMessages++;
//                // publish a message every second
//                // Thread.sleep(1000);
//            }
//
//            logger.info("COMPLETED PUBLISHING MESSAGES");
//            // wait for 1 minute then start the listener after all messages are published
//            Thread.sleep(10000);
//            registry.getListenerContainer(GROUP_ID).start();
//        };
//    }
//
//    @KafkaListener(id = GROUP_ID, topics = "trade_topic", groupId = GROUP_ID)
//    public void kafkaListener(Trade trade) throws Exception {
//         logger.info("Received Message in group trade_consumer: {}", trade);
//        // save the trade into the h2 database
//        // tradeService.saveTrade(trade);
//        // logger.info("Trade successfully saved into h2 database");
//        // logger.info("Trades in h2 db: {}", tradeService.fetchAllTrades());
//        // now try to pipe data to snowflake
//        snowpipeClient.pipeDataToSnowflake(trade);
//    }


//    @Bean
//    public ApplicationRunner runner(KafkaTemplate<Long, Trade> template) {
//        return args -> {
//            String jdbcUrl = "jdbc:snowflake://" + accountName + ".snowflakecomputing.com/";
//
//            try {
//                // Load the Snowflake JDBC driver
//                Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
//
//                // Establish connection
//                Connection connection = DriverManager.getConnection(jdbcUrl, SnowflakeConfig.getSnowflakeConnectionProperties());
//
//                // Create statement
//                Statement statement = connection.createStatement();
//
//                // Execute query
//                String query = "SELECT * FROM " + tableName;
//                ResultSet resultSet = statement.executeQuery(query);
//
//                // Process result
//                while (resultSet.next()) {
//                    String account = resultSet.getString("ACCOUNT");
//                    Long quantity = resultSet.getLong("QUANTITY");
//                    // ... (repeat for other columns)
//                    System.out.println("Account: " + account + ", quantity: " + quantity);
//                }
//
//                // Close resources
//                resultSet.close();
//                statement.close();
//                connection.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        };
//    }

//    @Bean
//    public ApplicationRunner runner(KafkaTemplate<Long, Trade> template) {
//        return args -> {
//
//            // create sample trade
//            Trade trade = new Trade(14107, "JEN_ACCOUNT","1301.TW",311);
//            logger.info("Sending trade...");
//            template.send("trade_topic", 311L, trade);
//        };
//    }
//
//    @KafkaListener(topics = "trade_topic", groupId = GROUP_ID)
//    public void kafkaListener(Trade trade) throws Exception {
//        logger.info("Received Message in group trade_consumer: {}", trade);
//        // save the trade into the h2 database
//        tradeService.saveTrade(trade);
//        logger.info("Trade successfully saved into h2 database");
//        logger.info("Trades in h2 db: {}", tradeService.fetchAllTrades());
//        // now try to pipe data to snowflake
//        snowpipeClient.pipeDataToSnowflake(trade);
//    }
}
