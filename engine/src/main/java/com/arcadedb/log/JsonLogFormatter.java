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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.LogRecord;

/**
 * Emits one compact JSON object per log line. Reuses {@link LogFormatter#formatMessage(LogRecord)} so
 * the MessageFormat {@code {0}} / printf {@code %s} dual-style substitution is identical to the text
 * formatter. Correlation fields (requestId, db, traceId, spanId) are read from {@link LogManager} and
 * included only when present, so the JSON stays minimal when tracing/correlation is off. Built with the
 * in-tree {@link JSONObject} (no new dependency); its {@code toString()} is compact and single-line, so
 * even a multi-line stack trace stays JSON-escaped on a single physical line.
 *
 * @author Luca Garulli
 */
public class JsonLogFormatter extends LogFormatter {
  private static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

  @Override
  public String format(final LogRecord record) {
    final JSONObject json = new JSONObject();
    json.put("timestamp", ISO.format(LocalDateTime.now()));
    json.put("level", record.getLevel().getName());
    json.put("logger", record.getLoggerName());
    json.put("thread", Thread.currentThread().getName());
    json.put("message", formatMessage(record));

    final LogManager.Correlation c = LogManager.instance().getCorrelation();
    if (c != null) {
      if (c.requestId() != null)
        json.put("requestId", c.requestId());
      if (c.database() != null)
        json.put("db", c.database());
      if (c.traceId() != null)
        json.put("traceId", c.traceId());
      if (c.spanId() != null)
        json.put("spanId", c.spanId());
    }

    if (record.getThrown() != null) {
      final StringWriter sw = new StringWriter();
      try (final PrintWriter pw = new PrintWriter(sw)) {
        record.getThrown().printStackTrace(pw);
      }
      json.put("exception", sw.toString());
    }

    return json + "\n";
  }
}
