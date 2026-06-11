package com.arcadedb.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class LocalDatabaseQueryMetricsTest {
  private final String dbPath = "./target/databases/LocalDatabaseQueryMetricsTest";

  @AfterEach
  void reset() {
    QueryMetricsRecorder.Holder.register(null);
    ProtocolContext.clear();
  }

  @Test
  void queryAndCommandAreRecordedWithProtocolAndType() {
    final AtomicReference<String> lastQuery = new AtomicReference<>();
    final AtomicReference<String> lastCommand = new AtomicReference<>();
    QueryMetricsRecorder.Holder.register((protocol, database, language, type, nanos) -> {
      if ("query".equals(type))
        lastQuery.set(protocol + "|" + language + "|" + type);
      else if ("command".equals(type))
        lastCommand.set(protocol + "|" + language + "|" + type);
    });

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      if (factory.exists())
        factory.open().drop();
      try (final Database db = factory.create()) {
        ProtocolContext.set("test");
        db.command("sql", "CREATE DOCUMENT TYPE Doc");
        db.query("sql", "SELECT FROM Doc");
      }
    }

    assertThat(lastCommand.get()).isEqualTo("test|sql|command");
    assertThat(lastQuery.get()).isEqualTo("test|sql|query");
  }
}
