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
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.CardinalityValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wire authorizer lets through only the instructions a client driver records: the steps of the traversal DSL and the
 * options of the traversal source. TinkerPop's translator maps an instruction operator onto any public method of that
 * name which returns a traversal, so an operator that is not a step must be found wherever a request can carry it, in the
 * request itself and in everything nested in it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ArcadeGremlinAuthorizerInstructionScanTest {
  /** The public methods of a traversal that the translator can reach by name and that are not steps. */
  private static final Set<String> NON_STEP_TRAVERSAL_METHODS = Set.of("addStep", "asAdmin", "clone", "iterate", "removeStep");
  /** The public methods of a traversal source that the translator can reach by name and that are not options. */
  private static final Set<String> NON_OPTION_SOURCE_METHODS    = Set.of("clone");

  private final GraphTraversalSource g = EmptyGraph.instance().traversal();

  @Test
  void driverTraversalsAreAccepted() {
    assertThat(unsupported(g.V().hasLabel("Person").out("knows").union(__.values("name"), __.count()).asAdmin().getBytecode())).isFalse();
    assertThat(unsupported(g.inject(List.of(1, 2), Map.of("k", "v")).asAdmin().getBytecode())).isFalse();
    assertThat(unsupported(g.V().has("age", P.gt(30).or(P.lt(10))).order().by("name").limit(5).asAdmin().getBytecode())).isFalse();
    assertThat(unsupported(g.addV("Person").property("name", "x").as("a").addE("knows").to("a").asAdmin().getBytecode())).isFalse();
    assertThat(unsupported(g.mergeV(Map.of("k", "v")).asAdmin().getBytecode())).isFalse();
    assertThat(unsupported(g.V().profile().asAdmin().getBytecode())).isFalse();
  }

  @Test
  void sourceInstructionsOfDriversAreAccepted() {
    final SubgraphStrategy strategy = SubgraphStrategy.build().vertices(__.hasLabel("Person")).create();
    assertThat(unsupported(g.withSideEffect("a", 1).withSack(1).withBulk(false).withPath().with("k", "v").withStrategies(strategy).V().asAdmin()
        .getBytecode())).isFalse();
    assertThat(unsupported(g.withoutStrategies(SubgraphStrategy.class).V().asAdmin().getBytecode())).isFalse();
  }

  @Test
  void cardinalityValueOfDriversIsAccepted() {
    assertThat(unsupported(g.addV("Person").property("tags", VertexProperty.Cardinality.set("a")).asAdmin().getBytecode())).isFalse();

    // As it arrives from the wire: a nested bytecode that carries a source instruction only
    final Bytecode bytecode = new Bytecode();
    bytecode.addStep("addV", "Person");
    bytecode.addStep("property", "tags", new Bytecode(CardinalityValueTraversal.class.getSimpleName(), "set", "a"));
    assertThat(unsupported(bytecode)).isFalse();
  }

  @Test
  void terminatingInstructionsOfDriversAreAccepted() {
    // What a driver records for iterate(): "discard" since TinkerPop 3.8, "none" before it
    final Bytecode discard = g.V().discard().asAdmin().getBytecode();
    assertThat(unsupported(discard)).isFalse();

    final Bytecode none = g.V().asAdmin().getBytecode().clone();
    none.addStep("none");
    assertThat(unsupported(none)).isFalse();
  }

  @Test
  void ioSpawnedFromSourceIsAccepted() {
    // The instruction is accepted; whether the user may run it is decided where the request executes
    assertThat(unsupported(g.io("graph.json").with(IO.writer, IO.graphson).write().asAdmin().getBytecode())).isFalse();
  }

  @Test
  void nonStepInstructionIsFound() {
    final Bytecode bytecode = g.V().hasLabel("Person").asAdmin().getBytecode().clone();
    bytecode.addStep("iterate");
    assertThat(unsupported(bytecode)).isTrue();
  }

  @Test
  void everyNonStepMethodIsFound() {
    for (final String operator : NON_STEP_TRAVERSAL_METHODS) {
      final Bytecode bytecode = g.V().asAdmin().getBytecode().clone();
      bytecode.addStep(operator);
      assertThat(unsupported(bytecode)).as("step instruction '%s'", operator).isTrue();
    }
    for (final String operator : NON_OPTION_SOURCE_METHODS) {
      final Bytecode bytecode = g.V().asAdmin().getBytecode().clone();
      bytecode.addSource(operator);
      assertThat(unsupported(bytecode)).as("source instruction '%s'", operator).isTrue();
    }
  }

  @Test
  void unknownOperatorIsFound() {
    final Bytecode bytecode = new Bytecode();
    bytecode.addStep("noSuchStep");
    assertThat(unsupported(bytecode)).isTrue();
  }

  @Test
  void instructionInNestedTraversalIsFound() {
    final Bytecode nested = new Bytecode();
    nested.addStep("V");
    nested.addStep("iterate");

    final Bytecode union = new Bytecode();
    union.addStep("V");
    union.addStep("union", nested);
    assertThat(unsupported(union)).isTrue();

    final Bytecode deeper = new Bytecode();
    deeper.addStep("V");
    final Bytecode middle = new Bytecode();
    middle.addStep("map", nested);
    deeper.addStep("filter", middle);
    assertThat(unsupported(deeper)).isTrue();
  }

  @Test
  void instructionBehindBindingOrInsideCollectionIsFound() {
    final Bytecode nested = new Bytecode();
    nested.addStep("iterate");

    final Bytecode binding = new Bytecode();
    binding.addStep("map", new Bytecode.Binding<>("x", nested));
    assertThat(unsupported(binding)).isTrue();

    final Bytecode parameter = new Bytecode();
    parameter.addStep("map", GValue.of("x", nested));
    assertThat(unsupported(parameter)).isTrue();

    final Bytecode list = new Bytecode();
    list.addStep("inject", List.of(1, Map.of("k", nested)));
    assertThat(unsupported(list)).isTrue();

    final Bytecode array = new Bytecode();
    array.addStep("inject", (Object) new Object[] { 1, nested });
    assertThat(unsupported(array)).isTrue();
  }

  @Test
  void instructionInStrategyConfigurationIsFound() {
    final Traversal<Vertex, Vertex> criterion = __.hasLabel("Person");
    criterion.asAdmin().getBytecode().addStep("iterate");
    final SubgraphStrategy strategy = SubgraphStrategy.build().vertices(criterion).create();
    assertThat(unsupported(g.withStrategies(strategy).V().asAdmin().getBytecode())).isTrue();
  }

  @Test
  void excessiveNestingFailsClosed() {
    Object value = "leaf";
    for (int i = 0; i < 100; i++)
      value = List.of(value);
    assertThat(unsupported(value)).isTrue();
    assertThat(ArcadeGremlinAuthorizer.unsupportedInstructionMessage(value, 0)).isEqualTo("The traversal is nested too deeply");

    // Nested bytecode, each level a valid step: the depth alone refuses it, and the message must not name an operator
    Bytecode nested = new Bytecode();
    nested.addStep("V");
    for (int i = 0; i < 100; i++) {
      final Bytecode outer = new Bytecode();
      outer.addStep("union", nested);
      nested = outer;
    }
    assertThat(ArcadeGremlinAuthorizer.unsupportedInstructionMessage(nested, 0)).isEqualTo("The traversal is nested too deeply");
  }

  @Test
  void messageNamesTheOperatorAndNothingElse() {
    final Bytecode bytecode = new Bytecode();
    bytecode.addStep("V");
    bytecode.addStep("iterate");
    assertThat(ArcadeGremlinAuthorizer.unsupportedInstructionMessage(bytecode, 0)).isEqualTo("Traversal instruction 'iterate' is not supported");

    final Bytecode source = new Bytecode();
    source.addSource("clone");
    assertThat(ArcadeGremlinAuthorizer.unsupportedInstructionMessage(source, 0)).isEqualTo("Traversal instruction 'clone' is not supported");

    // The operator comes from the client: a long one is cut
    final Bytecode longOperator = new Bytecode();
    longOperator.addStep("x".repeat(500));
    assertThat(ArcadeGremlinAuthorizer.unsupportedInstructionMessage(longOperator, 0)).hasSizeLessThan(100).contains("...");

    final Bytecode nullOperator = new Bytecode();
    nullOperator.addStep((String) null);
    assertThat(unsupported(nullOperator)).isTrue();

    assertThat(ArcadeGremlinAuthorizer.unsupportedInstructionMessage(g.V().out().asAdmin().getBytecode(), 0)).isNull();
  }

  /**
   * Census of what the translator can reach. The authority is the translator itself: it invokes, by operator name, the public
   * methods of the traversal returning a traversal, of the traversal source returning a traversal (the first step) and of the
   * traversal source returning a traversal source (the source instructions). Every name it can reach must be a step or
   * option a driver records, or be refused: a method added by a TinkerPop upgrade that is neither turns this red.
   */
  @Test
  void everyOperatorTheTranslatorReachesIsAStepOrRefused() {
    final Set<String> traversalMethods = new TreeSet<>();
    for (final Method method : DefaultGraphTraversal.class.getMethods())
      if (Traversal.class.isAssignableFrom(method.getReturnType()))
        traversalMethods.add(method.getName());

    final Set<String> spawnMethods = new TreeSet<>();
    final Set<String> sourceMethods = new TreeSet<>();
    for (final Method method : GraphTraversalSource.class.getMethods()) {
      if (Traversal.class.isAssignableFrom(method.getReturnType()))
        spawnMethods.add(method.getName());
      if (TraversalSource.class.isAssignableFrom(method.getReturnType()))
        sourceMethods.add(method.getName());
    }

    // The scan still reaches what it protects: if these are not found the census reads nothing
    assertThat(traversalMethods).as("methods of DefaultGraphTraversal that the translator invokes by name").contains("iterate", "out", "has", "addV", "values");
    assertThat(spawnMethods).as("methods of GraphTraversalSource that the translator invokes to spawn a traversal").contains("V", "addV", "io");
    assertThat(sourceMethods).as("methods of GraphTraversalSource that the translator invokes for a source instruction")
        .contains("withStrategies", "withSideEffect", "clone");

    final Set<String> refusedSteps = new TreeSet<>();
    for (final String name : traversalMethods)
      if (unsupportedStep(name))
        refusedSteps.add(name);
    assertThat(refusedSteps).as("step operators refused among those the translator reaches on a traversal").isEqualTo(NON_STEP_TRAVERSAL_METHODS);

    final Set<String> refusedSpawns = new TreeSet<>();
    for (final String name : spawnMethods)
      if (unsupportedStep(name))
        refusedSpawns.add(name);
    assertThat(refusedSpawns).as("step operators refused among those the translator reaches on a traversal source").isEmpty();

    final Set<String> refusedSources = new TreeSet<>();
    for (final String name : sourceMethods) {
      final Bytecode bytecode = new Bytecode();
      bytecode.addSource(name);
      if (unsupported(bytecode))
        refusedSources.add(name);
    }
    assertThat(refusedSources).as("source operators refused among those the translator reaches on a traversal source").isEqualTo(NON_OPTION_SOURCE_METHODS);
  }

  private static boolean unsupportedStep(final String operator) {
    final Bytecode bytecode = new Bytecode();
    bytecode.addStep(operator);
    return unsupported(bytecode);
  }

  private static boolean unsupported(final Object value) {
    return ArcadeGremlinAuthorizer.unsupportedInstructionMessage(value, 0) != null;
  }
}
