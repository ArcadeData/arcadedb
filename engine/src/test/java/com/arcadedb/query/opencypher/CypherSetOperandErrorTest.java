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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The refusal of a right-hand side that is neither a map, a node nor a relationship names the operator it was written with
 * (issue #6832): {@code SET n = 5} and {@code SET n += 5} are refused the same way, and the message tells which of the two the
 * statement wrote, as the user has to find it in a statement of many clauses. Neo4j refuses both too, with its own wording, which
 * is why the two cases are asserted on the message and not on Neo4j's.
 */
class CypherSetOperandErrorTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.transaction(() -> database.command("opencypher", "CREATE (:T {id: 1})").close());
  }

  @Test
  void aReplaceMapOfAScalarNamesTheReplaceOperator() {
    assertThatThrownBy(() -> run("MATCH (n:T) SET n = 5")).rootCause().hasMessageContaining("right-hand side of '='")
        .hasMessageContaining("but was Long");
  }

  @Test
  void aMergeMapOfAScalarNamesTheMergeOperator() {
    assertThatThrownBy(() -> run("MATCH (n:T) SET n += 'x'")).rootCause().hasMessageContaining("right-hand side of '+='")
        .hasMessageContaining("but was String");
  }

  private void run(final String statement) {
    database.transaction(() -> database.command("opencypher", statement).close());
  }
}
