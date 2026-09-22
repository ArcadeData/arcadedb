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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.database.QueryMetricsRecorder;
import com.arcadedb.database.QueryTracer;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7418: {@code SQLQueryEngine.analyze(query).execute(...)} - the entry point VectorSearch, HybridSearch and
 * the full-text SEARCH_INDEX function use to run their generated SQL - ran outside {@link QueryMetricsRecorder} and
 * {@link QueryTracer}, unlike {@code database.query()}/{@code command()}. So vector, hybrid and full-text search
 * traffic was invisible in {@code arcadedb.query.duration} and in traces, on every wire protocol.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7418AnalyzedQueryMetricsTest extends TestHelper {

  @AfterEach
  void reset() {
    QueryMetricsRecorder.Holder.register(null);
    QueryTracer.Holder.register(null);
    ProtocolContext.clear();
  }

  @Test
  void idempotentStatementIsRecordedAsQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "INSERT INTO Doc SET name = 'a'");
    });

    final AtomicReference<String> recorded = new AtomicReference<>();
    final AtomicInteger spansOpened = new AtomicInteger();
    QueryMetricsRecorder.Holder.register((protocol, database, language, type, nanos) -> recorded.set(protocol + "|" + language + "|" + type));
    QueryTracer.Holder.register((protocol, database, language, type, query) -> {
      spansOpened.incrementAndGet();
      return QueryTracer.Span.NO_OP;
    });

    database.transaction(() -> {
      ProtocolContext.set("test");
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("SELECT FROM Doc");
      assertThat(analyzed.isIdempotent()).isTrue();
      try (final ResultSet resultSet = analyzed.execute(Map.of())) {
        assertThat(resultSet.hasNext()).isTrue();
      }
    });

    assertThat(recorded.get()).isEqualTo("test|sql|query");
    assertThat(spansOpened.get()).isEqualTo(1);
  }

  @Test
  void nonIdempotentStatementIsRecordedAsCommand() {
    database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE Doc2"));

    final AtomicReference<String> recorded = new AtomicReference<>();
    QueryMetricsRecorder.Holder.register((protocol, database, language, type, nanos) -> recorded.set(protocol + "|" + language + "|" + type));

    database.transaction(() -> {
      ProtocolContext.set("test");
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("INSERT INTO Doc2 SET name = 'a'");
      assertThat(analyzed.isIdempotent()).isFalse();
      try (final ResultSet resultSet = analyzed.execute(Map.of())) {
        assertThat(resultSet.hasNext()).isTrue();
      }
    });

    assertThat(recorded.get()).isEqualTo("test|sql|command");
  }
}
