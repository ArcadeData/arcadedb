package com.arcadedb.server.kafka;

import com.arcadedb.log.LogManager;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;
import java.util.logging.Level;

public class KafkaClientConfiguration {
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    protected static final String ARCADEDB_TOPIC_PREFIX = "arcadedbcdc";

    private static String getValueOrDefault(String configurationKey, String defaultValue) {
        String envConfigKey = String.format("KAFKA_%s", configurationKey.toUpperCase().replaceAll("\\.", "_"));
        LogManager.instance().log(KafkaClientConfiguration.class, Level.INFO, "looking for env variable '%s'", envConfigKey);
        return System.getenv(envConfigKey) != null ? System.getenv(envConfigKey) : defaultValue;
    }

    public static Properties getKafkaClientConfiguration() {
        Properties clientConfiguration = new Properties();
        clientConfiguration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getValueOrDefault(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kafka-bootstrap.localhost:9092"));
        clientConfiguration.put(CommonClientConfigs.CLIENT_ID_CONFIG, getValueOrDefault(CommonClientConfigs.CLIENT_ID_CONFIG, "admin-arcadedb-kafka-client"));
        clientConfiguration.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, getValueOrDefault(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL));
        clientConfiguration.put(SaslConfigs.SASL_MECHANISM, getValueOrDefault(SaslConfigs.SASL_MECHANISM, ""));
        clientConfiguration.put(SaslConfigs.SASL_JAAS_CONFIG, getValueOrDefault(SaslConfigs.SASL_JAAS_CONFIG, ""));
        clientConfiguration.put(SCHEMA_REGISTRY_URL, getValueOrDefault(SCHEMA_REGISTRY_URL, "http://df-schema-registry:8081"));

        return clientConfiguration;
    }
}
