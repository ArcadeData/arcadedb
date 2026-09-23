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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the exact statements the workload and the checkpoint send over HTTP against an embedded database, so a syntax
 * or semantics mistake shows up here instead of as a mysterious failure 20 minutes into a container run.
 */
@Tag("chaos")
class ChaosSchemaIT {
  private static final String PATH = "./target/chaos-schema-it";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(PATH));
    factory = new DatabaseFactory(PATH);
    database = factory.create();
    for (final String ddl : ChaosSchema.DDL)
      database.command("sql", ddl);
  }

  @AfterEach
  void tearDown() {
    database.drop();
    factory.close();
  }

  private void single(final long key) {
    database.transaction(() -> database.command("sql", ChaosSchema.INSERT_SINGLE, ChaosSchema.singleParams(key)));
  }

  private void pair(final long key, final long target) {
    database.command("sqlscript", ChaosSchema.INSERT_PAIR, ChaosSchema.pairParams(key, target));
  }

  private List<long[]> page(final long last, final int size) {
    final List<long[]> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", ChaosSchema.page(size), Map.of("last", last))) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        rows.add(new long[] { ((Number) row.getProperty("id")).longValue(), ((Number) row.getProperty("e")).longValue() });
      }
    }
    return rows;
  }

  private long count(final String sql) {
    try (final ResultSet resultSet = database.query("sql", sql)) {
      return ((Number) resultSet.next().getProperty("c")).longValue();
    }
  }

  @Test
  void ddlIsIdempotent() {
    for (final String ddl : ChaosSchema.DDL)
      database.command("sql", ddl);
    assertThat(database.getSchema().existsType("ChaosOp")).isTrue();
    assertThat(database.getSchema().existsType("NEXT")).isTrue();
  }

  @Test
  void singleInsertHasNoEdge() {
    final long key = Ledger.key(0, 0);
    single(key);
    assertThat(page(-1, ChaosSchema.PAGE_SIZE)).containsExactly(new long[] { key, 0 });
  }

  @Test
  void pairInsertCreatesOneOutgoingEdge() {
    final long target = Ledger.key(0, 0);
    final long pairKey = Ledger.key(1, 0);
    single(target);
    pair(pairKey, target);
    assertThat(page(-1, ChaosSchema.PAGE_SIZE)).containsExactly(new long[] { target, 0 }, new long[] { pairKey, 1 });
    assertThat(count(ChaosSchema.COUNT_OPS)).isEqualTo(2);
    assertThat(count(ChaosSchema.COUNT_EDGES)).isEqualTo(1);
  }

  @Test
  void pageResumesAfterTheLastKeyInKeyOrder() {
    single(Ledger.key(1, 0));
    single(Ledger.key(0, 2));
    single(Ledger.key(0, 0));
    single(Ledger.key(0, 1));
    assertThat(page(Ledger.key(0, 0), ChaosSchema.PAGE_SIZE)).extracting(row -> row[0])
        .containsExactly(Ledger.key(0, 1), Ledger.key(0, 2), Ledger.key(1, 0));
  }

  @Test
  void pageSizeLimitsRows() {
    for (int i = 0; i < 5; i++)
      single(Ledger.key(0, i));
    assertThat(page(-1, 2)).hasSize(2);
  }

  @Test
  void duplicateKeyIsRejected() {
    final long key = Ledger.key(0, 0);
    single(key);
    assertThatThrownBy(() -> single(key)).hasStackTraceContaining("DuplicatedKeyException");
  }
}
