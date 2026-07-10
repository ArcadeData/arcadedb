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
package com.arcadedb.gremlin;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5201: a Gremlin query using a Groovy closure (e.g. {@code filter { ... }}) is
 * rejected by the secure gremlin-lang (java) engine. The parse failure must surface as a
 * {@link CommandParsingException} (a client error) carrying the real parser message, not as a generic
 * {@link CommandExecutionException} that the HTTP layer degrades to a misleading 500 "Error on transaction
 * commit".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinClosureParseErrorTest {
  @Test
  void closureQueryThrowsParsingExceptionNotExecutionException() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlinclosure");
    try {
      graph.getDatabase().getSchema().createVertexType("A");
      graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("A").set("id", 1).save());

      assertThatThrownBy(() -> graph.gremlin(
          "g.V().hasLabel('A').filter { it.get().property('id').value() > 0 }.values('id').fold()").execute())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Error on parsing gremlin query");
    } finally {
      graph.drop();
      FileUtils.deleteRecursively(new File("./target/testgremlinclosure"));
    }
  }

  @Test
  void equivalentNonClosureFormStillWorks() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlinclosure2");
    try {
      graph.getDatabase().getSchema().createVertexType("A");
      graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("A").set("id", 1).save());

      // The workaround documented in the issue: predicate form instead of a Groovy closure.
      assertThat(graph.gremlin("g.V().hasLabel('A').has('id', gt(0)).values('id').fold()").execute().hasNext()).isTrue();
    } finally {
      graph.drop();
      FileUtils.deleteRecursively(new File("./target/testgremlinclosure2"));
    }
  }
}
