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
package com.arcadedb.gremlin.query;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6359, item 3: an engine that could not start because TinkerPop is missing from the
 * classpath has to say so, instead of leaving a {@code NoClassDefFoundError} under a message that points at the engine.
 * <p>
 * The way to reach it is a plain build mistake - consuming {@code arcadedb-gremlin}'s ordinary jar where the
 * {@code shaded} uber-jar was meant, which is what Maven substitutes for a classified dependency whose module shares
 * the reactor but has not reached the {@code package} phase.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinQueryEngineFactoryClasspathHintTest {

  @Test
  void aMissingTinkerPopIsNamedAsAClasspathProblem() {
    final String hint = GremlinQueryEngineFactory.classpathHint(
        new RuntimeException("wrapped", new NoClassDefFoundError("org/apache/tinkerpop/gremlin/structure/Graph")));

    assertThat(hint).contains("TinkerPop libraries are not on the classpath");
    assertThat(hint).as("the missing class is named in the form the user would recognise")
        .contains("org.apache.tinkerpop.gremlin.structure.Graph");
    assertThat(hint).contains("shaded");
  }

  @Test
  void anUnrelatedFailureAddsNothing() {
    assertThat(GremlinQueryEngineFactory.classpathHint(new IllegalStateException("no graph for you"))).isEmpty();
    // A missing class that is NOT TinkerPop is a different problem and must not be mislabelled as this one.
    assertThat(GremlinQueryEngineFactory.classpathHint(new NoClassDefFoundError("com/example/Whatever"))).isEmpty();
  }
}
