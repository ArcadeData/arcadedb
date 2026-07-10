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

import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

class LogFormatterTraceTagTest {

  @AfterEach
  void reset() {
    LogManager.instance().clearCorrelation();
    GlobalConfiguration.SERVER_LOG_INCLUDE_TRACE.setValue(false);
  }

  @Test
  void appendsTraceTagWhenEnabledAndTraceActive() {
    GlobalConfiguration.SERVER_LOG_INCLUDE_TRACE.setValue(true);
    LogManager.instance().setCorrelation("req-1", "mydb", "trace-abc", "span-xyz");

    final LogFormatter formatter = new LogFormatter();
    final LogRecord record = new LogRecord(Level.INFO, "hello");
    record.setLoggerName("com.arcadedb.Test");

    assertThat(formatter.format(record)).contains("[traceId=trace-abc]");
  }

  @Test
  void noTraceTagWhenDisabledEvenWithActiveTrace() {
    GlobalConfiguration.SERVER_LOG_INCLUDE_TRACE.setValue(false);
    LogManager.instance().setCorrelation("req-1", "mydb", "trace-abc", "span-xyz");

    final LogFormatter formatter = new LogFormatter();
    final LogRecord record = new LogRecord(Level.INFO, "hello");
    record.setLoggerName("com.arcadedb.Test");

    assertThat(formatter.format(record)).doesNotContain("traceId");
  }

  @Test
  void noTraceTagWhenEnabledButNoTraceActive() {
    GlobalConfiguration.SERVER_LOG_INCLUDE_TRACE.setValue(true);
    // no correlation set -> traceId null

    final LogFormatter formatter = new LogFormatter();
    final LogRecord record = new LogRecord(Level.INFO, "hello");
    record.setLoggerName("com.arcadedb.Test");

    assertThat(formatter.format(record)).doesNotContain("traceId");
  }
}
