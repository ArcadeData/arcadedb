package com.arcadedb.server.kafka;

import com.arcadedb.database.Record;
import com.raft.arcadedb.cdc.Message;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.UUID;

public class KafkaRecordUtil {
    @NotNull
    protected static Message createMessage(KafkaEventListener.RecordEvents afterRecordEvent, Record record) {
        return Message.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setTimestamp(String.valueOf(Instant.now().toEpochMilli()))
                .setEventType(afterRecordEvent.toString())
                .setEventPayload(record.toJSON().toString())
                .setUsername(record.getDatabase().getCurrentUserName())
                .setEntityName(getEntityName(record))
                .setEntityId(record.getIdentity().toString())
                .build();
    }

    protected static String getEntityName(Record record) {
        if (record != null) {
            return record.asDocument().getTypeName();
        } else {
            return "";
        }
    }
}
