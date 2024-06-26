package org.galatea.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.galatea.kafka.CommonConfig;
import org.galatea.kafka.Trade;
import org.galatea.kafka.TradeDeserializer;
import org.galatea.kafka.TradeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.galatea.kafka.Util.readConfig;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public ConsumerFactory<Long, Trade> consumerFactory() {

        Properties props = new Properties();
        try {
            props = readConfig("C:/Users/jennl/IdeaProjects/Spring_Kafka_Sample_Application/src/main/resources/client.properties");
        } catch (IOException e) {
            logger.error("An error was encountered while parsing the config file.", e);
        }


        Map<String, Object> propsMap = new HashMap<>();

        for (final String name: props.stringPropertyNames())
            propsMap.put(name, props.getProperty(name));

        propsMap.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                CommonConfig.GROUP_ID);
        propsMap.put(
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
                CommonConfig.GROUP_ID);
        propsMap.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class);
        propsMap.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                TradeDeserializer.class);
        // By default, consumer only consumes events published after it started because auto.offset.reset=latest by default.
        // Since we are starting consumer AFTER publisher publishes all message we need to set this explicitly
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(propsMap);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, Trade> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<Long, Trade> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setAutoStartup(false);
        return factory;
    }
}
