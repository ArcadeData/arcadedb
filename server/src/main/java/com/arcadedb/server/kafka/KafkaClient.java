package com.arcadedb.server.kafka;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import com.raft.arcadedb.cdc.Message;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class KafkaClient {
    @EqualsAndHashCode
    @AllArgsConstructor
    static class DatabaseEntry {
        String databaseName;
        String username;
    }

    private final AdminClient adminClient;
    private final ConcurrentHashMap<DatabaseEntry, Producer> producerCache = new ConcurrentHashMap<>();

    public KafkaClient() {
        this.adminClient = AdminClient.create(KafkaClientConfiguration.getKafkaClientConfiguration());
    }

    public void createTopicIfNotExists(String topicName) {
        try {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                int numPartitions = 1; // todo refactor magic number
                short replicationFactor = 2; // todo refactor magic number

                NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                LogManager.instance().log(this, Level.INFO, "Topic created: %s", topicName);
            } else {
                LogManager.instance().log(this, Level.INFO, "Topic already exists: %s", topicName);
            }

        } catch (InterruptedException | ExecutionException exception) {
            if (!(exception.getCause() instanceof TopicExistsException)) {
                LogManager.instance().log(this, Level.SEVERE, exception.getMessage());
            } else {
                LogManager.instance().log(this, Level.INFO, "Topic already exists: %s", topicName);
            }
        }
    }

    public void sendMessage(String database, String userName, Message message) {
        producerCache.computeIfAbsent(new DatabaseEntry(database, userName), d -> new Producer(getTopicNameForDatabase(d.databaseName, d.username)));
        producerCache.get(new DatabaseEntry(database, userName)).send(message);
    }

    // Removes any special characters from the database name. Ensuring we are not breaking downstream ingestion.
    protected String normalizeDatabaseName(String databaseName) {
        return databaseName.replaceAll("[^a-zA-Z]", "").toLowerCase();
    }

    protected String getTopicNameForDatabase(String databaseName, String databaseUsername) {
        return String.format("%s--%s--%s", KafkaClientConfiguration.ARCADEDB_TOPIC_PREFIX, databaseUsername, normalizeDatabaseName(databaseName));
    }

    protected void shutdown() {
        for (Map.Entry<DatabaseEntry, Producer> entry : this.producerCache.entrySet()) {
            entry.getValue().flush();
        }
    }
}
