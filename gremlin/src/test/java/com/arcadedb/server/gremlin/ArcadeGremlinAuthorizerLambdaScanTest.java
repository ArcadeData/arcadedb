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
package com.arcadedb.server.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wire authorizer must find a lambda wherever a request can carry one, not only as a direct step argument.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeGremlinAuthorizerLambdaScanTest {
  private final GraphTraversalSource g = EmptyGraph.instance().traversal();

  @Test
  void traversalWithoutLambdaIsClean() {
    assertThat(scan(g.V().hasLabel("Person").out("knows").union(__.values("name"), __.count()).asAdmin().getBytecode())).isFalse();
    assertThat(scan(g.inject(List.of(1, 2), Map.of("k", "v")).asAdmin().getBytecode())).isFalse();
  }

  @Test
  void directLambdaIsFound() {
    assertThat(scan(g.V().map(Lambda.function("it.get()")).asAdmin().getBytecode())).isTrue();
  }

  @Test
  void lambdaInNestedTraversalIsFound() {
    assertThat(scan(g.V().union(__.identity(), __.map(__.map(Lambda.function("it.get()")))).asAdmin().getBytecode())).isTrue();
  }

  @Test
  void lambdaBehindBindingOrInsideCollectionIsFound() {
    final Bytecode binding = new Bytecode();
    binding.addStep("map", new Bytecode.Binding<>("x", Lambda.function("it.get()")));
    assertThat(scan(binding)).isTrue();

    final Bytecode list = new Bytecode();
    list.addStep("inject", List.of(1, Map.of("k", Lambda.function("it.get()"))));
    assertThat(scan(list)).isTrue();

    final Bytecode array = new Bytecode();
    array.addStep("inject", (Object) new Object[] { 1, Lambda.function("it.get()") });
    assertThat(scan(array)).isTrue();
  }

  @Test
  void lambdaInStrategyConfigurationIsFound() {
    final SubgraphStrategy strategy = SubgraphStrategy.build().vertices(__.filter(Lambda.predicate("true"))).create();
    assertThat(scan(g.withStrategies(strategy).V().asAdmin().getBytecode())).isTrue();
  }

  @Test
  void excessiveNestingFailsClosed() {
    Object value = "leaf";
    for (int i = 0; i < 100; i++)
      value = List.of(value);
    assertThat(scan(value)).isTrue();
  }

  private static boolean scan(final Object value) {
    return ArcadeGremlinAuthorizer.containsLambda(value, 0);
  }
}
