package org.galatea.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class SnowflakeConfig {

    private static Logger logger = LoggerFactory.getLogger(SnowflakeConfig.class);

    private static String SNOWFLAKE_PROFILE_PATH = "C:/Users/jennl/IdeaProjects/Spring_Kafka_Sample_Application/src/main/resources/profile.json";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Properties getSnowflakeConnectionProperties() {
        Properties properties = new Properties();

        try {
            Iterator<Map.Entry<String, JsonNode>> propIterator = mapper.readTree(new String(Files.readAllBytes(Paths.get(SNOWFLAKE_PROFILE_PATH)))).fields();
            while (propIterator.hasNext()) {
                Map.Entry<String, JsonNode> property = propIterator.next();
                properties.put(property.getKey(), property.getValue().asText());
            }
        } catch (Exception e) {
            logger.error("There was a problem parsing the profile.json file.", e);
        }


        return properties;
    }
}
