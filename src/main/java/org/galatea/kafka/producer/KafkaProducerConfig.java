package org.galatea.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.galatea.kafka.Trade;
import org.galatea.kafka.TradeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static org.galatea.kafka.Util.readConfig;

@Configuration
public class KafkaProducerConfig {

    private static Logger logger = LoggerFactory.getLogger(KafkaProducerConfig.class);

//    @Bean
//    public ProducerFactory<Long, Trade> producerFactory() {
//
//        Map<String, Object> configProps = new HashMap<>();
//        configProps.put(
//                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
//                CommonConfig.BOOTSTRAP_SERVER);
//        configProps.put(
//                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//                LongSerializer.class);
//        configProps.put(
//                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                TradeSerializer.class);
//        configProps.put(
//                ProducerConfig.BATCH_SIZE_CONFIG,
//                1000);
//        return new DefaultKafkaProducerFactory<>(configProps);
//    }
//
//    @Bean
//    public KafkaTemplate<Long, Trade> kafkaTemplate() {
//        return new KafkaTemplate<>(producerFactory());
//    }

    @Bean(name = "KafkaProducer")
    public KafkaProducer<Long, Trade> kafkaProducer() {
        Properties props = new Properties();
        try {
            props = readConfig("C:/Users/jennl/IdeaProjects/Spring_Kafka_Sample_Application/src/main/resources/client.properties");
            props.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    LongSerializer.class);
            props.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    TradeSerializer.class);
        } catch (IOException e) {
            logger.error("An error was encountered while parsing the config file.", e);
        }
        return new KafkaProducer<>(props);
    }

}