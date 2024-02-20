package com.arcadedb.server.kafka;

import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.raft.arcadedb.cdc.Message;

public class KafkaEventListener implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {
    enum RecordEvents {
        AFTER_RECORD_UPDATE("UPDATE"),
        AFTER_RECORD_DELETE("DELETE"),
        AFTER_RECORD_CREATE("CREATE");

        private final String value;

        RecordEvents(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private final KafkaClient client;
    private final String databaseName;
    private final String databaseUsername;

    public KafkaEventListener(final KafkaClient client, final String dbName, String databaseUsername) {
        this.client = client;
        this.databaseName = dbName;
        /*
        Users are allowed to not provide a username when they create a database. We need a username
        for downstream processing. Since the username is inferred from the topic name by down stream
        jobs/applications we need to provide a value when no value is specified during database creation.
         */
        this.databaseUsername = databaseUsername == null ? "admin" : databaseUsername;

        this.client.createTopicIfNotExists(this.client.getTopicNameForDatabase(this.databaseName, this.databaseUsername));
    }

    @Override
    public void onAfterCreate(Record record) {
        Message message = KafkaRecordUtil.createMessage(RecordEvents.AFTER_RECORD_CREATE, record);

        this.client.sendMessage(this.databaseName, this.databaseUsername, message);
    }

    @Override
    public void onAfterDelete(Record record) {
        Message message = KafkaRecordUtil.createMessage(RecordEvents.AFTER_RECORD_DELETE, record);

        this.client.sendMessage(this.databaseName, this.databaseUsername, message);
    }

    @Override
    public void onAfterUpdate(Record record) {
        Message message = KafkaRecordUtil.createMessage(RecordEvents.AFTER_RECORD_UPDATE, record);

        this.client.sendMessage(this.databaseName, this.databaseUsername, message);
    }
}
