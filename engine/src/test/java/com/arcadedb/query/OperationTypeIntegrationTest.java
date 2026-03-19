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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for semantic operation type detection via QueryEngine.analyze().
 * Tests SQL and OpenCypher through the full engine pipeline.
 */
class OperationTypeIntegrationTest extends TestHelper {

  // --- SQL via engine.analyze() ---

  @Test
  void sqlSelectViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("SELECT FROM V");
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void sqlInsertViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("INSERT INTO V SET name = 'test'");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.CREATE);
  }

  @Test
  void sqlUpdateViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("UPDATE V SET name = 'test'");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.UPDATE);
  }

  @Test
  void sqlUpsertViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze(
        "UPDATE V SET name = 'test' UPSERT WHERE name = 'test'");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
  }

  @Test
  void sqlDeleteViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("DELETE FROM V");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.DELETE);
  }

  @Test
  void sqlCreateTypeViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("CREATE VERTEX TYPE NewType");
    assertThat(analyzed.isDDL()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void sqlCreateIndexViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("CREATE INDEX ON V (name) UNIQUE");
    assertThat(analyzed.isDDL()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  // --- OpenCypher via engine.analyze() ---

  @Test
  void openCypherMatchViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze("MATCH (n) RETURN n");
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void openCypherCreateViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze(
        "CREATE (n:V {name: 'test'}) RETURN n");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
  }

  @Test
  void openCypherSetViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze(
        "MATCH (n:V) SET n.name = 'test' RETURN n");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.UPDATE);
  }

  @Test
  void openCypherDeleteViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze(
        "MATCH (n:V) DELETE n");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.DELETE);
  }

  @Test
  void openCypherMergeViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze(
        "MERGE (n:V {name: 'test'}) RETURN n");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
  }

  @Test
  void openCypherCreateConstraintViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze(
        "CREATE CONSTRAINT myConstraint FOR (n:V) REQUIRE n.name IS UNIQUE");
    assertThat(analyzed.isDDL()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void openCypherAdminViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze("SHOW USERS");
    assertThat(analyzed.isDDL()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.ADMIN);
  }

  @Test
  void openCypherRemoveViaEngine() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze(
        "MATCH (n:V) REMOVE n.name RETURN n");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.UPDATE);
  }

  // --- QueryTool semantic check: write queries must be detected as non-idempotent ---

  @Test
  void sqlInsertIsNotIdempotent() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("INSERT INTO V SET name = 'test'");
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Test
  void sqlUpdateIsNotIdempotent() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("UPDATE V SET name = 'test'");
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Test
  void sqlDeleteIsNotIdempotent() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("DELETE FROM V");
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Test
  void openCypherCreateIsNotIdempotent() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze(
        "CREATE (n:V {name: 'test'}) RETURN n");
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
  }
}
