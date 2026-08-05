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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers engine selection and the parse-versus-runtime error classification, which decides HTTP 400
 * against HTTP 500 (issues #5201 and #5219), plus the process-wide timeout field.
 */
class ArcadeGremlinEngineSelectionTest {

  private ArcadeGraph graph;
  private String      originalEngine;

  @BeforeEach
  void setup() {
    originalEngine = GlobalConfiguration.GREMLIN_ENGINE.getValueAsString();
    graph = ArcadeGraph.open("./target/test-gremlin-engine");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Alice"));
  }

  @AfterEach
  void teardown() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue(originalEngine);
    if (graph != null)
      graph.drop();
  }

  @Test
  void aGroovyClosureIsRejectedAsAParsingErrorUnderTheJavaEngine() {
    ((Database) graph.getDatabase()).getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "java");
    // Strict java mode must NOT fall back to the insecure Groovy engine (GHSA-wcm5-4wjm-9wj3).
    assertThatThrownBy(() -> graph.gremlin("g.V().filter { it.get().value('name') == 'Alice' }").execute())
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void nextOnAnEmptyTraversalIsARuntimeErrorNotAParsingError() {
    ((Database) graph.getDatabase()).getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "java");
    // Issue #5219: a NoSuchElementException surfaced during eager iteration must stay a
    // CommandExecutionException (HTTP 500), not be misreported as invalid syntax (HTTP 400).
    assertThatThrownBy(() -> graph.gremlin("g.V().has('name','NoSuchPerson').next()").execute())
        .isInstanceOf(CommandExecutionException.class)
        .isNotInstanceOf(CommandParsingException.class);
  }

  @Test
  void genuinelyInvalidSyntaxIsAParsingError() {
    ((Database) graph.getDatabase()).getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "java");
    assertThatThrownBy(() -> graph.gremlin("g.V().thisStepDoesNotExist()").execute())
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void anUnknownEngineNameIsRejected() {
    ((Database) graph.getDatabase()).getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "nonsense");
    assertThatThrownBy(() -> graph.gremlin("g.V().count()").execute())
        .isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void theJavaEngineExecutesAValidQuery() {
    ((Database) graph.getDatabase()).getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "java");
    assertThat((Long) graph.gremlin("g.V().count()").execute().nextIfAvailable().getProperty("result"))
        .isEqualTo(1L);
  }

  @Test
  void autoModeFallsBackToGroovyForAClosure() {
    ((Database) graph.getDatabase()).getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "auto");
    // 'auto' is documented as opting in to the Groovy fallback for compatibility.
    assertThat((Long) graph.gremlin("g.V().count()").execute().nextIfAvailable().getProperty("result"))
        .isEqualTo(1L);
  }

  /**
   * CHARACTERIZATION TEST OF A KNOWN DEFECT. This asserts the WRONG behavior on purpose, to pin it
   * and to detect if it silently changes. It is not a statement of the intended contract.
   * <p>
   * {@code ArcadeGremlin.timeout} is declared {@code private static Long} but assigned by the
   * INSTANCE method {@code setTimeout(long, TimeUnit)}, so a timeout set on one graph applies to
   * every ArcadeGremlin in the process.
   * <p>
   * WHEN THE FIELD IS MADE NON-STATIC, INVERT THIS TEST: the expectation becomes that {@code second}
   * does NOT observe {@code first}'s timeout, and the method should be renamed accordingly.
   */
  @Test
  void characterizesTheProcessWideTimeoutLeak() {
    final ArcadeGremlin first = graph.gremlin("g.V().count()");
    final ArcadeGremlin second = graph.gremlin("g.V().count()");
    first.setTimeout(1234, java.util.concurrent.TimeUnit.MILLISECONDS);
    assertThat(second.getTimeout())
        .as("timeout leaked across ArcadeGremlin instances via the static field")
        .isEqualTo(1234L);
  }
}
