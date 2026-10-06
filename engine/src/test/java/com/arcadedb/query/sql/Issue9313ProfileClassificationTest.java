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
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9313: {@code PROFILE <statement>} answered {@code isIdempotent()==true} and {@code [READ]} whatever it wrapped,
 * yet executes the wrapped statement. So {@code PROFILE INSERT} passed every read-only gate ({@code query()}, streaming,
 * MCP, HA follower routing). The classification must follow the wrapped statement; {@code EXPLAIN} never executes, so it
 * stays idempotent while still reporting the wrapped operation types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9313ProfileClassificationTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createDocumentType("Person"));
  }

  @Test
  void profileOfAWriteIsNotIdempotent() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("PROFILE INSERT INTO Person SET name = 'a'");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE).doesNotContain(OperationType.READ);
  }

  @Test
  void profileOfUpdateAndDeleteIsNotIdempotent() {
    assertThat(database.getQueryEngine("sql").analyze("PROFILE UPDATE Person SET name = 'b'").isIdempotent()).isFalse();
    assertThat(database.getQueryEngine("sql").analyze("PROFILE DELETE FROM Person").getOperationTypes()).contains(OperationType.DELETE);
  }

  @Test
  void profileOfASelectStaysIdempotent() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("PROFILE SELECT FROM Person");
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsOnly(OperationType.READ);
  }

  @Test
  void profileOfDdlIsDdl() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("PROFILE CREATE DOCUMENT TYPE Other");
    assertThat(analyzed.isDDL()).isTrue();
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Test
  void explainStaysIdempotentButReportsTheWrappedOperation() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("EXPLAIN INSERT INTO Person SET name = 'a'");
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
  }

  @Test
  void profileInsertIsRefusedByQuery() {
    assertThatThrownBy(() -> database.query("sql", "PROFILE INSERT INTO Person SET name = 'x'"))
        .isInstanceOf(QueryNotIdempotentException.class);
    assertThat(database.countType("Person", false)).isZero();
  }

  @Test
  void profileSelectStillWorksOnQuery() {
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'"));
    try (final ResultSet rs = database.query("sql", "PROFILE SELECT FROM Person")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  @Test
  void cypherProfileOfAWriteIsRefusedByQuery() {
    assertThatThrownBy(() -> database.query("opencypher", "PROFILE CREATE (:Person {name: 'x'})"))
        .isInstanceOf(QueryNotIdempotentException.class);
    assertThat(database.countType("Person", false)).isZero();
  }

  @Test
  void cypherProfileOfAWriteIsClassifiedAsAWrite() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze("PROFILE CREATE (:Person {name: 'x'})");
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Test
  void cypherProfileOfAReadIsAllowedOnQuery() {
    try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (p:Person) RETURN p")) {
      rs.stream().count();
    }
  }
}
