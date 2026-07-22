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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Security regression test for advisory GHSA-wcm5-4wjm-9wj3: authenticated RCE via a silent Groovy fallback
 * in the default (documented-secure) {@code java} Gremlin engine.
 * <p>
 * The secure gremlin-lang ({@code java}) engine cannot parse a Groovy closure such as
 * {@code filter { ... }}: the closure body is arbitrary attacker-controlled Groovy code, which is the RCE
 * primitive. Before the fix, {@code java} mode refused the fallback to the insecure Groovy engine only when
 * the request carried no parameters. Any request with a single (even unused) parameter re-enabled the silent
 * fallback, so a read-only user could run arbitrary Groovy (hence OS commands) by attaching a dummy parameter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinGroovyFallbackRCETest {
  private static final String DB_PATH = "./target/testgremlin-ghsa-wcm5";

  // The gremlin-lang (java) engine rejects Groovy closures; the closure body is the arbitrary-code vector.
  private static final String CLOSURE_QUERY =
      "g.V().hasLabel('A').filter { it.get().property('id').value() > 0 }.values('id').fold()";

  @Test
  void groovyClosureWithParametersIsBlockedInDefaultJavaMode() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue("java");
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try {
      graph.getDatabase().getSchema().createVertexType("A");
      graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("A").set("id", 1).save());

      // A single (unused) parameter must NOT re-open the Groovy fallback in strict java mode.
      // Before the fix, this executed the Groovy closure (RCE). After the fix, it is a parse error.
      assertThatThrownBy(() -> graph.gremlin(CLOSURE_QUERY)
          .setParameters(Map.of("dummy", 1))
          .execute())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Error on parsing gremlin query");
    } finally {
      graph.drop();
      GlobalConfiguration.GREMLIN_ENGINE.reset();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  @Test
  void groovyClosureWithoutParametersStaysBlockedInJavaMode() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue("java");
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try {
      graph.getDatabase().getSchema().createVertexType("A");
      graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("A").set("id", 1).save());

      // Baseline that was already safe: no parameters => no fallback.
      assertThatThrownBy(() -> graph.gremlin(CLOSURE_QUERY).execute())
          .isInstanceOf(CommandParsingException.class);
    } finally {
      graph.drop();
      GlobalConfiguration.GREMLIN_ENGINE.reset();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  @Test
  void legitParameterizedQueryStillExecutesInJavaMode() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue("java");
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try {
      graph.getDatabase().getSchema().createVertexType("A");
      graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("A").set("id", 42).save());

      // A parameterized query the gremlin-lang engine can parse must keep working after removing the fallback.
      final ResultSet rs = graph.gremlin("g.V().hasLabel('A').has('id', p0).values('id').fold()")
          .setParameters(Map.of("p0", 42))
          .execute();
      assertThat(rs.hasNext()).isTrue();
    } finally {
      graph.drop();
      GlobalConfiguration.GREMLIN_ENGINE.reset();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  @Test
  void autoModeRemainsTheExplicitOptInForGroovyFallback() {
    // 'auto' is documented as the opt-in that "falls back to Groovy if needed (not recommended for
    // security-critical deployments)". The fix must only harden 'java', leaving 'auto' behavior intact.
    GlobalConfiguration.GREMLIN_ENGINE.setValue("auto");
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try {
      graph.getDatabase().getSchema().createVertexType("A");
      graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("A").set("id", 1).save());

      final ResultSet rs = graph.gremlin(CLOSURE_QUERY).execute();
      assertThat(rs.hasNext()).isTrue();
    } finally {
      graph.drop();
      GlobalConfiguration.GREMLIN_ENGINE.reset();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }
}
