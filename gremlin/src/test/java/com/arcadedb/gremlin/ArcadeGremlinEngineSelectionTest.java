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

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers engine selection and the parse-versus-runtime error classification, which decides HTTP 400
 * against HTTP 500 (issues #5201 and #5219), plus the removal of the dead, process-wide timeout API
 * (issue #5842).
 */
class ArcadeGremlinEngineSelectionTest {

  private ArcadeGraph graph;

  // NOTE: no save/restore of GlobalConfiguration.GREMLIN_ENGINE here. Every test in this class writes
  // the PER-DATABASE ContextConfiguration (((Database) graph.getDatabase()).getConfiguration().setValue(...)),
  // never the global GlobalConfiguration.GREMLIN_ENGINE field - so a global guard would be protecting
  // something these tests never mutate. It is also unnecessary: setup()/teardown() open a fresh
  // ArcadeGraph backed by a freshly created database on every test and graph.drop() deletes that
  // database's files afterwards, so the per-database config from one test can never carry over to the
  // next.
  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-engine");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Alice"));
  }

  @AfterEach
  void teardown() {
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
    // 'auto' is documented as opting in to the Groovy fallback for compatibility. A Groovy closure is
    // exactly the shape gremlin-lang (the "java" engine) rejects with a ScriptException - see
    // aGroovyClosureIsRejectedAsAParsingErrorUnderTheJavaEngine above, same query shape - so this query
    // genuinely forces the fallback path in executeStatement(boolean) rather than merely succeeding on
    // the java engine under a different config value.
    assertThat((Long) graph.gremlin("g.V().filter { it.get().value('name') == 'Alice' }.count()").execute()
        .nextIfAvailable().getProperty("result"))
        .isEqualTo(1L);
  }

  /**
   * Regression test for issue #5842: {@code ArcadeGremlin.setTimeout(long, TimeUnit)} /
   * {@code getTimeout()} were a complete no-op (nothing ever read the backing field) and, worse, the
   * backing field was {@code private static Long} assigned by an instance method, so a timeout set on
   * one graph leaked to every {@code ArcadeGremlin} in the process.
   * <p>
   * No caller anywhere in the codebase used this API (verified by a repo-wide search), and there is no
   * existing hook in {@code executeStatement()} to actually enforce a per-query Gremlin timeout, so the
   * dead API was removed outright (YAGNI) rather than wired up speculatively. This test guards against
   * it silently reappearing.
   */
  @Test
  void setTimeoutAndGetTimeoutAreNoLongerPartOfThePublicApi() {
    assertThatThrownBy(() -> ArcadeGremlin.class.getDeclaredMethod("setTimeout", long.class, TimeUnit.class))
        .as("setTimeout(long, TimeUnit) must not exist: it was a no-op that also leaked across instances")
        .isInstanceOf(NoSuchMethodException.class);

    assertThatThrownBy(() -> ArcadeGremlin.class.getDeclaredMethod("getTimeout"))
        .as("getTimeout() must not exist alongside a removed setTimeout()")
        .isInstanceOf(NoSuchMethodException.class);

    assertThatThrownBy(() -> ArcadeGremlin.class.getDeclaredField("timeout"))
        .as("the process-wide static 'timeout' field must be gone")
        .isInstanceOf(NoSuchFieldException.class);
  }
}
