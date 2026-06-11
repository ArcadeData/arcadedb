package com.arcadedb.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class QueryMetricsRecorderTest {
  @AfterEach
  void reset() {
    QueryMetricsRecorder.Holder.register(null); // back to NO_OP
  }

  @Test
  void defaultIsNoOpAndStartNanosReturnsZero() {
    assertThat(QueryMetricsRecorder.Holder.get()).isSameAs(QueryMetricsRecorder.NO_OP);
    assertThat(QueryMetricsRecorder.Holder.startNanos()).isZero();
  }

  @Test
  void registeredRecorderReceivesRecordWhenTimingActive() {
    final AtomicReference<String> captured = new AtomicReference<>();
    QueryMetricsRecorder.Holder.register(
        (protocol, database, language, type, durationNanos) -> captured.set(protocol + "/" + database + "/" + language + "/" + type));

    final long start = QueryMetricsRecorder.Holder.startNanos();
    assertThat(start).isNotZero(); // an active recorder makes startNanos() time
    QueryMetricsRecorder.Holder.record(start, "db1", "sql", "query");

    assertThat(captured.get()).isEqualTo(ProtocolContext.get() + "/db1/sql/query");
  }

  @Test
  void recordWithZeroStartIsIgnored() {
    final AtomicReference<Boolean> called = new AtomicReference<>(false);
    QueryMetricsRecorder.Holder.register((p, d, l, t, n) -> called.set(true));
    QueryMetricsRecorder.Holder.record(0L, "db1", "sql", "query");
    assertThat(called.get()).isFalse();
  }
}
