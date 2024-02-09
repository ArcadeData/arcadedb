package com.arcadedb.server.kafka;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.log.LogManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class StreamDBSubscriptionService extends Thread {
    private final ConcurrentMap<String, KafkaEventListener> registeredEventListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DatabaseInternal> databases;
    private final KafkaClient kafkaClient = new KafkaClient();
    private final String dbNamePattern;
    private final long serviceTimeoutMillis;
    private final String databaseUsername;

    public StreamDBSubscriptionService(final String dbNamePattern, final ConcurrentMap<String, DatabaseInternal> databases, long serviceTimeoutMillis) {
        this.databases = databases;
        this.dbNamePattern = dbNamePattern;
        this.serviceTimeoutMillis = serviceTimeoutMillis;
        this.databaseUsername = System.getenv("STREAM_DEFAULT_DATABASE_USERNAME") == null ?  "admin" : System.getenv("STREAM_DEFAULT_DATABASE_USERNAME");
    }

    @Override
    public void run() {
        while (true) {
            for (Map.Entry<String, DatabaseInternal> entry : this.databases.entrySet()) {
                if (entry.getKey().matches(this.dbNamePattern) && !registeredEventListeners.containsKey(entry.getKey())) {
                    String databaseName = entry.getKey();
                    String databaseUsername = getOrDefaultUsername(entry.getValue()); // This can be null. This is handled in Event listener.

                    LogManager.instance().log(this, Level.INFO, String.format("Adding event listeners for database: '%s', and user: %s", databaseName, databaseUsername));
                    KafkaEventListener listener = registeredEventListeners.computeIfAbsent(entry.getKey(), k -> new KafkaEventListener(this.kafkaClient, databaseName, databaseUsername));
                    entry.getValue().getEvents().registerListener((AfterRecordCreateListener) listener).registerListener((AfterRecordUpdateListener) listener)
                            .registerListener((AfterRecordDeleteListener) listener);
                }

                try {
                    Thread.sleep(this.serviceTimeoutMillis);
                } catch (InterruptedException ignored) {
                    LogManager.instance().log(this, Level.SEVERE, "Shutting down %s. Flushing messages. ", this.getName());
                    this.kafkaClient.shutdown();
                }
            }
        }
    }

    private String getOrDefaultUsername(DatabaseInternal database) {
        return database.getCurrentUserName() == null ? this.databaseUsername : database.getCurrentUserName();
    }
}
