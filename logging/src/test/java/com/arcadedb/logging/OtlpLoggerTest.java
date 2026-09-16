/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.logging;

import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The decorator's contract, which is mostly about what it must not break.
 *
 * <p>The exporting half needs a collector and belongs in an integration test; what is pinned here is
 * that local logging survives every way exporting can go wrong, because that is the property an
 * operator is trusting when they turn this on.
 */
class OtlpLoggerTest {

  /** Records what it was asked to log, standing in for whatever the configuration selected. */
  private static class RecordingLogger implements Logger {
    private final List<String> lines = new ArrayList<>();

    @Override
    @SuppressWarnings("PMD.ExcessiveParameterList")
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
        final Object arg17) {
      lines.add(message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      lines.add(message);
    }

    private boolean flushed;

    @Override
    public void flush() {
      flushed = true;
    }
  }

  /** Points at a port nothing is listening on, which is what a collector being down looks like. */
  private static OtlpLogger exportingNowhere(final Logger delegate) {
    return new OtlpLogger(delegate, "http://localhost:1");
  }

  @Test
  void writesToTheConfiguredLoggerAsWellAsExporting() {
    final RecordingLogger delegate = new RecordingLogger();

    exportingNowhere(delegate).log(this, Level.INFO, "a message", null, null);

    assertThat(delegate.lines).containsExactly("a message");
  }

  @Test
  void keepsLoggingLocallyWhenTheCollectorCannotBeReached() {
    // The property an operator is trusting: turning export on must not be able to cost them their logs.
    final RecordingLogger delegate = new RecordingLogger();
    final OtlpLogger logger = exportingNowhere(delegate);

    for (int i = 0; i < 5; i++)
      logger.log(this, Level.SEVERE, "line " + i, new IllegalStateException("boom"), null);

    assertThat(delegate.lines).hasSize(5);
  }

  @Test
  void aMessageWhoseArgumentsDoNotFitIsStillLogged() {
    final RecordingLogger delegate = new RecordingLogger();

    exportingNowhere(delegate).log(this, Level.WARNING, "%d and %d", null, null, "not a number");

    assertThat(delegate.lines).containsExactly("%d and %d");
  }

  @Test
  void flushingReachesTheDelegateEvenWhenTheExportFlushCannotComplete() {
    final RecordingLogger delegate = new RecordingLogger();

    exportingNowhere(delegate).flush();

    assertThat(delegate.flushed).isTrue();
  }

  /** Reads back what was emitted, instead of sending it to a collector. */
  private static InMemoryLogRecordExporter exported;

  private static OtlpLogger exportingInMemory(final Logger delegate) {
    exported = InMemoryLogRecordExporter.create();
    return new OtlpLogger(delegate, SdkLoggerProvider.builder()
        .addLogRecordProcessor(SimpleLogRecordProcessor.create(exported))
        .build());
  }

  @Test
  void aRecordIsExportedWithItsMessageAndSeverity() {
    exportingInMemory(new RecordingLogger()).log(this, Level.WARNING, "disk is filling up", null, null);

    assertThat(exported.getFinishedLogRecordItems()).singleElement().satisfies(record -> {
      assertThat(record.getBodyValue().asString()).isEqualTo("disk is filling up");
      assertThat(record.getSeverity()).isEqualTo(Severity.WARN);
      assertThat(record.getSeverityText()).isEqualTo("WARNING");
    });
  }

  @Test
  void theArgumentsAreAppliedBeforeExporting() {
    // The collector should receive the sentence, not the format string.
    exportingInMemory(new RecordingLogger()).log(this, Level.INFO, "%s took %dms", null, null, "compaction", 1200);

    assertThat(exported.getFinishedLogRecordItems()).singleElement()
        .satisfies(record -> assertThat(record.getBodyValue().asString()).isEqualTo("compaction took 1200ms"));
  }

  @Test
  void theRecordSaysWhichClassAskedForIt() {
    exportingInMemory(new RecordingLogger()).log(OtlpLoggerTest.class, Level.INFO, "a message", null, null);

    assertThat(exported.getFinishedLogRecordItems()).singleElement().satisfies(record ->
        assertThat(record.getAttributes().asMap())
            .containsEntry(io.opentelemetry.api.common.AttributeKey.stringKey("logger.name"),
                OtlpLoggerTest.class.getName()));
  }

  @Test
  void anExceptionIsCarriedAsAnAttribute() {
    exportingInMemory(new RecordingLogger())
        .log(this, Level.SEVERE, "it broke", new IllegalStateException("boom"), null);

    assertThat(exported.getFinishedLogRecordItems()).singleElement().satisfies(record ->
        assertThat(record.getAttributes().asMap())
            .containsEntry(io.opentelemetry.api.common.AttributeKey.stringKey("exception.type"),
                IllegalStateException.class.getName()));
  }

  @Test
  void everyLevelMapsOntoASeverity() {
    final OtlpLogger logger = exportingInMemory(new RecordingLogger());

    logger.log(this, Level.SEVERE, "severe", null, null);
    logger.log(this, Level.WARNING, "warning", null, null);
    logger.log(this, Level.INFO, "info", null, null);
    logger.log(this, Level.FINE, "fine", null, null);
    logger.log(this, Level.FINEST, "finest", null, null);

    assertThat(exported.getFinishedLogRecordItems()).extracting(LogRecordData::getSeverity)
        .containsExactly(Severity.ERROR, Severity.WARN, Severity.INFO, Severity.DEBUG, Severity.TRACE);
  }

  @Test
  void aRecordFromTheSdkItselfIsNotExported() {
    final OtlpLogger logger = exportingInMemory(new RecordingLogger());

    logger.log(io.opentelemetry.sdk.logs.SdkLoggerProvider.class, Level.SEVERE, "Failed to export logs", null, null);
    logger.log(this, Level.INFO, "an ordinary line", null, null);

    assertThat(exported.getFinishedLogRecordItems()).extracting(r -> r.getBodyValue().asString())
        .containsExactly("an ordinary line");
  }

  @Test
  void closingReleasesTheProviderAndIsIdempotent() {
    // The provider owns a batch thread and a connection. A logger that is replaced, or a server that
    // stops, must not leave either behind.
    final InMemoryLogRecordExporter exporter = InMemoryLogRecordExporter.create();
    final SdkLoggerProvider provider = SdkLoggerProvider.builder()
        .addLogRecordProcessor(SimpleLogRecordProcessor.create(exporter)).build();
    final OtlpLogger logger = new OtlpLogger(new RecordingLogger(), provider);

    logger.close();
    logger.close();

    // Shut down means shut down: the SDK drops records emitted afterwards rather than queueing them.
    logger.log(this, Level.INFO, "after close", null, null);
    assertThat(exporter.getFinishedLogRecordItems()).isEmpty();
  }

  @Test
  void closingStillWritesLocally() {
    final RecordingLogger delegate = new RecordingLogger();
    final OtlpLogger logger = exportingNowhere(delegate);

    logger.close();
    logger.log(this, Level.WARNING, "the collector is gone, the console is not", null, null);

    assertThat(delegate.lines).containsExactly("the collector is gone, the console is not");
  }

  @Test
  void aRecordCarriesTheCorrelationOfTheRequestThatProducedIt() {
    // The reason to export logs from the server at all: the line lands next to the span it came from.
    LogManager.instance().setCorrelation("req-7", "mydb", "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
    try {
      exportingInMemory(new RecordingLogger()).log(this, Level.INFO, "a query ran", null, null);

      assertThat(exported.getFinishedLogRecordItems()).singleElement().satisfies(record ->
          assertThat(record.getAttributes().asMap())
              .containsEntry(io.opentelemetry.api.common.AttributeKey.stringKey("arcadedb.requestId"), "req-7")
              .containsEntry(io.opentelemetry.api.common.AttributeKey.stringKey("arcadedb.database"), "mydb")
              .containsEntry(io.opentelemetry.api.common.AttributeKey.stringKey("arcadedb.traceId"),
                  "4bf92f3577b34da6a3ce929d0e0e4736")
              .containsEntry(io.opentelemetry.api.common.AttributeKey.stringKey("arcadedb.spanId"),
                  "00f067aa0ba902b7"));
    } finally {
      LogManager.instance().clearCorrelation();
    }
  }

  @Test
  void correlationThatIsNotThereDoesNotBecomeEmptyAttributes() {
    LogManager.instance().clearCorrelation();

    exportingInMemory(new RecordingLogger()).log(this, Level.INFO, "no request behind this one", null, null);

    assertThat(exported.getFinishedLogRecordItems()).singleElement().satisfies(record ->
        assertThat(record.getAttributes().asMap().keySet())
            .noneMatch(key -> key.getKey().startsWith("arcadedb.")));
  }

  @Test
  void theExportersOwnFailuresAreLoggedLocallyAndNotExported() {
    // Otherwise the failure path feeds itself: the collector is down, the SDK says so through this
    // logger, and that line is queued for the collector that is down.
    final RecordingLogger delegate = new RecordingLogger();

    exportingNowhere(delegate).log(io.opentelemetry.sdk.logs.SdkLoggerProvider.class, Level.SEVERE,
        "Failed to export logs", new IllegalStateException("connection refused"), null);

    assertThat(delegate.lines).containsExactly("Failed to export logs");
  }

}
