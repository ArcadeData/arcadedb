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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.TypeMismatchException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11850: the run-time refusals of a CREATE or MERGE pattern are errors in the statement or its data, the kind Neo4j reports
 * as client errors, but they were plain {@code CommandExecutionException}s, which the server answers with HTTP 500. They are now
 * the {@code TypeMismatchException} (a value that is not a node or a relationship) and the {@code InvalidArgumentException} (a null
 * MERGE property) the server answers with 400. Both still extend {@code CommandExecutionException}.
 */
class Issue11850WritePatternClientErrorsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> database.command("opencypher", "CREATE (:P {i: 1})"));
  }

  @Test
  void aNullMergePropertyIsAnInvalidArgument() {
    final Map<String, Object> params = new HashMap<>();
    params.put("p", null);
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MERGE (n:P {i: $p}) RETURN n", params)))
        .isInstanceOf(InvalidArgumentException.class).hasMessageContaining("MergeReadOwnWrites");
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("opencypher", "UNWIND [1, null] AS x MERGE (n:P {i: x}) RETURN n.i")))
        .isInstanceOf(InvalidArgumentException.class).hasMessageContaining("MergeReadOwnWrites");
  }

  @Test
  void aMissingEndpointOrRelationshipIsATypeMismatch() {
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("opencypher", "OPTIONAL MATCH (a:Nope) CREATE (a)-[:R]->(:P) RETURN 1")))
        .isInstanceOf(TypeMismatchException.class).hasMessageContaining("Expected the value NULL to be of type NODE");
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("opencypher", "OPTIONAL MATCH (a:Nope) MERGE (a)-[:R]->(b:P) RETURN 1")))
        .isInstanceOf(TypeMismatchException.class).hasMessageContaining("Expected the value NULL to be of type NODE");
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (a:P {i: 1}) WITH a, null AS r MERGE (a)-[r:R]->(:P) RETURN 1")))
        .isInstanceOf(TypeMismatchException.class).hasMessageContaining("of type RELATIONSHIP");
  }

  @Test
  void nothingWasWritten() {
    try {
      database.transaction(() -> database.command("opencypher", "UNWIND [2, null] AS x MERGE (n:P {i: x}) RETURN n.i"));
    } catch (final InvalidArgumentException expected) {
      // the second row is refused, and the statement with it
    }
    assertThat(database.countType("P", false)).isEqualTo(1);
  }
}
