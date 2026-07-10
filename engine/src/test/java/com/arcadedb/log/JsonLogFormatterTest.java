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
package com.arcadedb.log;

import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

class JsonLogFormatterTest {

  @AfterEach
  void clear() {
    LogManager.instance().clearCorrelation();
  }

  @Test
  void emitsSingleLineJsonWithMessageAndLevel() {
    final JsonLogFormatter formatter = new JsonLogFormatter();
    final LogRecord record = new LogRecord(Level.INFO, "hello world");
    record.setLoggerName("com.arcadedb.Test");

    final String line = formatter.format(record);

    assertThat(line).endsWith("\n");
    assertThat(line.trim()).doesNotContain("\n"); // exactly one line
    final JSONObject json = new JSONObject(line.trim());
    assertThat(json.getString("level")).isEqualTo("INFO");
    assertThat(json.getString("message")).isEqualTo("hello world");
    assertThat(json.getString("logger")).isEqualTo("com.arcadedb.Test");
    assertThat(json.has("timestamp")).isTrue();
    assertThat(json.has("thread")).isTrue();
  }

  @Test
  void omitsCorrelationFieldsWhenAbsent() {
    final JsonLogFormatter formatter = new JsonLogFormatter();
    final LogRecord record = new LogRecord(Level.INFO, "no correlation");
    record.setLoggerName("com.arcadedb.Test");

    final JSONObject json = new JSONObject(formatter.format(record).trim());
    assertThat(json.has("requestId")).isFalse();
    assertThat(json.has("traceId")).isFalse();
    assertThat(json.has("db")).isFalse();
    assertThat(json.has("spanId")).isFalse();
  }

  @Test
  void includesCorrelationFieldsWhenPresent() {
    LogManager.instance().setCorrelation("req-1", "mydb", "trace-abc", "span-xyz");
    final JsonLogFormatter formatter = new JsonLogFormatter();
    final LogRecord record = new LogRecord(Level.WARNING, "warn msg");
    record.setLoggerName("com.arcadedb.Test");

    final JSONObject json = new JSONObject(formatter.format(record).trim());
    assertThat(json.getString("traceId")).isEqualTo("trace-abc");
    assertThat(json.getString("spanId")).isEqualTo("span-xyz");
    assertThat(json.getString("requestId")).isEqualTo("req-1");
    assertThat(json.getString("db")).isEqualTo("mydb");
  }

  @Test
  void appliesMessageFormatPlaceholdersInsideJson() {
    final JsonLogFormatter formatter = new JsonLogFormatter();
    final LogRecord record = new LogRecord(Level.INFO, "value is {0}");
    record.setParameters(new Object[] { 42 });
    record.setLoggerName("com.arcadedb.Test");

    final JSONObject json = new JSONObject(formatter.format(record).trim());
    assertThat(json.getString("message")).isEqualTo("value is 42");
  }

  @Test
  void appliesPrintfPlaceholdersInsideJson() {
    final JsonLogFormatter formatter = new JsonLogFormatter();
    final LogRecord record = new LogRecord(Level.INFO, "value is %s");
    record.setParameters(new Object[] { "x" });
    record.setLoggerName("com.arcadedb.Test");

    final JSONObject json = new JSONObject(formatter.format(record).trim());
    assertThat(json.getString("message")).isEqualTo("value is x");
  }

  @Test
  void includesExceptionAsSingleLineField() {
    final JsonLogFormatter formatter = new JsonLogFormatter();
    final LogRecord record = new LogRecord(Level.SEVERE, "boom");
    record.setLoggerName("com.arcadedb.Test");
    record.setThrown(new IllegalStateException("kaboom"));

    final String line = formatter.format(record);
    assertThat(line).endsWith("\n");
    assertThat(line.trim()).doesNotContain("\n"); // stack trace must stay JSON-escaped on one line
    final JSONObject json = new JSONObject(line.trim());
    assertThat(json.getString("exception")).contains("IllegalStateException").contains("kaboom");
  }
}
