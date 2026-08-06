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

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers engine selection and the parse-versus-runtime error classification, which decides HTTP 400
 * against HTTP 500 (issues #5201 and #5219), plus the process-wide timeout field.
 */
class ArcadeGremlinEngineSelectionTest {

  private ArcadeGraph graph;
  private Long        originalTimeout;

  // NOTE: no save/restore of GlobalConfiguration.GREMLIN_ENGINE here. Every test in this class writes
  // the PER-DATABASE ContextConfiguration (((Database) graph.getDatabase()).getConfiguration().setValue(...)),
  // never the global GlobalConfiguration.GREMLIN_ENGINE field - so a global guard would be protecting
  // something these tests never mutate. It is also unnecessary: setup()/teardown() open a fresh
  // ArcadeGraph backed by a freshly created database on every test and graph.drop() deletes that
  // database's files afterwards, so the per-database config from one test can never carry over to the
  // next. What genuinely IS process-wide and must be restored is ArcadeGremlin.timeout (see
  // originalTimeout below and characterizesTheProcessWideTimeoutLeak).
  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-engine");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Alice"));
    originalTimeout = graph.gremlin("g.V().count()").getTimeout();
  }

  @AfterEach
  void teardown() {
    // The drop must happen even if the restore throws. Otherwise a failing restore leaves
    // ./target/test-gremlin-engine on disk and the NEXT run fails in setup() against a database that
    // already has the Person type, masking the real cause behind a confusing cascade.
    try {
      restoreProcessWideTimeout();
    } finally {
      if (graph != null)
        graph.drop();
    }
  }

  /**
   * Restores {@code ArcadeGremlin.timeout} to whatever it held before this test, INCLUDING when that
   * value was {@code null}, which is its default.
   * <p>
   * Reflection is required rather than the public setter: {@code setTimeout(long, TimeUnit)} takes a
   * primitive and therefore cannot express null. Restoring only when the previous value was non-null
   * would leave {@code characterizesTheProcessWideTimeoutLeak}'s 1234 in place for the remainder of
   * the Surefire fork (forkCount=1, reuseForks=true), leaking into every later test class. That is
   * harmless only for as long as nothing reads the field - so the isolation guarantee here must not
   * depend on the very defect this class characterizes staying unfixed.
   * <p>
   * If the field is ever made non-static (the fix this class asks for), this call throws and the
   * test class must be updated alongside it. Failing loudly is the intent.
   */
  private void restoreProcessWideTimeout() {
    try {
      final Field field = ArcadeGremlin.class.getDeclaredField("timeout");
      field.setAccessible(true);
      field.set(null, originalTimeout);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to restore the process-wide ArcadeGremlin.timeout", e);
    }
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
   * CHARACTERIZATION TEST OF A KNOWN DEFECT. This asserts the WRONG behavior on purpose, to pin it
   * and to detect if it silently changes. It is not a statement of the intended contract.
   * <p>
   * {@code ArcadeGremlin.timeout} is declared {@code private static Long} but assigned by the
   * INSTANCE method {@code setTimeout(long, TimeUnit)}, so a timeout set on one graph applies to
   * every ArcadeGremlin in the process.
   * <p>
   * Tracked as issue #5842, which also records that nothing reads the field, so {@code setTimeout}
   * is currently a no-op regardless of the static-versus-instance question.
   * <p>
   * WHEN THE FIELD IS MADE NON-STATIC, INVERT THIS TEST: the expectation becomes that {@code second}
   * does NOT observe {@code first}'s timeout, and the method should be renamed accordingly.
   */
  @Test
  void characterizesTheProcessWideTimeoutLeak() {
    final ArcadeGremlin first = graph.gremlin("g.V().count()");
    final ArcadeGremlin second = graph.gremlin("g.V().count()");
    first.setTimeout(1234, TimeUnit.MILLISECONDS);
    assertThat(second.getTimeout())
        .as("timeout leaked across ArcadeGremlin instances via the static field")
        .isEqualTo(1234L);
  }
}
