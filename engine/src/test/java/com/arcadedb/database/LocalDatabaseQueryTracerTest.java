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
package com.arcadedb.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the engine query/command boundary opens a {@link QueryTracer} span carrying the
 * originating protocol (from {@link ProtocolContext}), language, type and query text - the single
 * instrumentation point every wire protocol funnels through. Uses a fake tracer (no Micrometer).
 */
class LocalDatabaseQueryTracerTest {
  private final String dbPath = "./target/databases/LocalDatabaseQueryTracerTest";

  @AfterEach
  void reset() {
    QueryTracer.Holder.register(null);
    ProtocolContext.clear();
  }

  @Test
  void queryAndCommandOpenSpansWithProtocolLanguageTypeAndQuery() {
    final List<String> begun = new ArrayList<>();
    final List<String> ended = new ArrayList<>();
    QueryTracer.Holder.register((protocol, database, language, type, query) -> {
      begun.add(protocol + "|" + database + "|" + language + "|" + type + "|" + query);
      return () -> ended.add(type);
    });

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      if (factory.exists())
        factory.open().drop();
      try (final Database db = factory.create()) {
        ProtocolContext.set("postgres");
        db.command("sql", "CREATE DOCUMENT TYPE Doc");
        db.query("sql", "SELECT FROM Doc");
      }
    }

    assertThat(begun).contains(
        "postgres|LocalDatabaseQueryTracerTest|sql|command|CREATE DOCUMENT TYPE Doc",
        "postgres|LocalDatabaseQueryTracerTest|sql|query|SELECT FROM Doc");
    // Every opened span must have been closed (try-with-resources at the boundary).
    assertThat(ended).contains("command", "query");
    assertThat(ended).hasSameSizeAs(begun);
  }

  @Test
  void noTracerRegisteredIsANoOp() {
    // Default holder is NO_OP: queries run without opening any span and without error.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      if (factory.exists())
        factory.open().drop();
      try (final Database db = factory.create()) {
        db.command("sql", "CREATE DOCUMENT TYPE Doc");
        assertThat(db.query("sql", "SELECT FROM Doc").stream().count()).isZero();
      }
    }
    assertThat(QueryTracer.Holder.get()).isSameAs(QueryTracer.NO_OP);
  }
}
