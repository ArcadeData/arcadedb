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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Opcodes;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10415 (and #10393): every read of a graph view goes through a pin.
 * <p>
 * A view publishes a base numbered afresh on every rebuild, so a caller that asks it several questions - a node id, then the
 * neighbours, then their RIDs - must ask them all of one snapshot, or a rebuild in between answers the later ones about other
 * vertices, without failing. The pin is per caller: {@code GraphTraversalProviderRegistry.findProvider(Database, ...)} hands out
 * the live view, because the planner caches what it gets and must not cache a snapshot. A call site that forgets to pin compiles
 * and runs, and is wrong only while a rebuild races it. So:
 * <ul>
 *   <li>every lookup of a view either passes the statement's command context (an overload taking a {@code CommandContext},
 *   which returns the statement's pin), or is in a method that pins through {@code GraphViewPins}, unless its method is listed
 *   here with the reason it reads no more than once or is the planner's;</li>
 *   <li>every query operator or step that holds a view in a field pins it when it executes, unless listed here.</li>
 * </ul>
 * The census ranges over the authorities (#11288), in the compiled classes of every module ({@link CompiledClasses}): a lookup
 * is a call of a {@code findProvider...} method of the registry, told pinned or not by the descriptor of the overload; an
 * operator is a class whose super types reach {@code AbstractPhysicalOperator} or {@code AbstractExecutionStep}, and it holds a
 * view when one of its fields is declared {@code GraphTraversalProvider}. The source-text census it replaces read the engine and
 * the gremlin module by a hardcoded path, took a statement as pinned when its variable was named {@code context} or {@code ctx},
 * and an operator only when it extended a base directly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphViewPinCensusTest {
  private static final String REGISTRY        = "com/arcadedb/graph/GraphTraversalProviderRegistry";
  private static final String LOOKUP          = "findProvider";
  private static final String PINS            = "com/arcadedb/graph/GraphViewPins";
  private static final String COMMAND_CONTEXT = "Lcom/arcadedb/query/sql/executor/CommandContext;";
  private static final String PROVIDER        = "Lcom/arcadedb/graph/GraphTraversalProvider;";
  private static final Set<String> OPERATOR_BASES = Set.of("com/arcadedb/query/opencypher/executor/operators/AbstractPhysicalOperator",
      "com/arcadedb/query/sql/executor/AbstractExecutionStep");

  /** Lookups that need no pin, by {@code class#method}, or by class for every method of it, with the reason. */
  private static final Map<String, String> UNPINNED_LOOKUPS = new TreeMap<>(Map.of(
      "query/opencypher/optimizer/CypherOptimizer",
      "the planner: its plans are cached, so they hold the live view and the operators pin it when they run",
      "query/opencypher/optimizer/statistics/StatisticsProvider", "the planner's estimates: one count per lookup, read once",
      "query/sql/executor/MatchExecutionPlanner", "the SQL planner: MatchGAVFusedStep pins the view when it runs",
      "query/opencypher/executor/CypherExecutionPlan$PairJoinStatistics#edges",
      "the planner's row estimate: one countAllEdges() per lookup",
      "graph/olap/ShortestPathFinder#find", "captures one snapshot of the view itself (currentSnapshot()) and reads only through it",
      "query/opencypher/procedures/algo/AbstractAlgoProcedure#findReadyProvider",
      "its caller findProvider(Database, String[], CommandContext) pins what it returns"));

  /** Operators and steps holding a view in a field without pinning it themselves, by class, with the reason. */
  private static final Map<String, String> UNPINNED_HOLDERS = new TreeMap<>(Map.of(
      "query/opencypher/executor/steps/GAVOneHopScanStep",
      "built for one execution by CypherExecutionPlan.tryGAVOneHopScan(), with the statement's pin"));

  @Test
  void everyLookupOfAViewIsPinned() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> unpinned = unpinnedLookupsOf(classes);

    final Set<String> offenders = new TreeSet<>();
    final Set<String> exemptionsUsed = new TreeSet<>();
    for (final String site : unpinned) {
      final String exemption = exemptionOf(site);
      if (exemption == null)
        offenders.add(site);
      else
        exemptionsUsed.add(exemption);
    }

    assertThat(offenders).as("a view looked up without a pin is read in whatever snapshot it serves at each call, and a rebuild "
        + "between two calls answers the later ones about other vertices (issue #10415). To resolve: pass the statement's "
        + "CommandContext (findProvider(context, ...)), or wrap the lookup in GraphViewPins.pin(context, ...) / "
        + "GraphViewPins.pinAlone(...); if the lookup reads the view once or belongs to the planner, add it to UNPINNED_LOOKUPS "
        + "with the reason").isEmpty();
    assertThat(exemptionsUsed).as("every exempted lookup is still an unpinned one: drop the others")
        .containsExactlyInAnyOrderElementsOf(UNPINNED_LOOKUPS.keySet());
  }

  @Test
  void everyOperatorHoldingAViewPinsItWhenItRuns() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> holders = holdersOf(classes);
    assertThat(holders).as("the census found the operators that hold a view").contains(
        "query/opencypher/executor/operators/GAVExpandAll", "query/opencypher/executor/operators/GAVExpandInto",
        "query/opencypher/executor/operators/GAVFusedChainOperator", "query/sql/executor/MatchGAVFusedStep",
        "query/opencypher/executor/steps/MatchRelationshipStep");

    final Set<String> pinning = pinningClassesOf(classes);
    final Set<String> unpinned = new TreeSet<>();
    for (final String holder : holders)
      if (!pinning.contains(holder) && !UNPINNED_HOLDERS.containsKey(holder))
        unpinned.add(holder);

    assertThat(unpinned).as("a plan is cached and holds the live view: an operator that reads it without pinning it reads a "
        + "different snapshot at each call (issue #10415). To resolve: read the view through GraphViewPins.pin(context, provider) "
        + "when the operator executes; if it is built for one execution with a pinned view, add it to UNPINNED_HOLDERS with the "
        + "reason").isEmpty();
    assertThat(holders).as("every exempted holder still holds a view").containsAll(UNPINNED_HOLDERS.keySet());
  }

  /** The {@code class#method} keys with a lookup that takes no command context, in a method that pins nothing. */
  private static Set<String> unpinnedLookupsOf(final Map<String, byte[]> classes) {
    final Map<String, Set<String>> lookups = CompiledClasses.callSitesOf(classes,
        (owner, name, desc) -> REGISTRY.equals(owner) && name.startsWith(LOOKUP) && !desc.contains(COMMAND_CONTEXT));
    final Set<String> pinned = CompiledClasses.callSitesOf(classes, (owner, name, desc) -> PINS.equals(owner)).keySet();
    final Set<String> unpinned = new TreeSet<>();
    for (final String site : lookups.keySet())
      if (!pinned.contains(site) && !site.startsWith(CompiledClasses.keyOf(REGISTRY + ".class") + "#"))
        unpinned.add(site);
    return unpinned;
  }

  /** The operators and steps, as {@link CompiledClasses} keys them, with a field declared {@code GraphTraversalProvider}. */
  private static Set<String> holdersOf(final Map<String, byte[]> classes) {
    final Set<String> operators = new HashSet<>();
    for (final String base : OPERATOR_BASES)
      operators.addAll(CompiledClasses.implementationsOf(base, classes));

    final Set<String> holders = new TreeSet<>();
    for (final Map.Entry<String, byte[]> entry : classes.entrySet()) {
      final ClassReader reader = new ClassReader(entry.getValue());
      if (!operators.contains(reader.getClassName()))
        continue;
      reader.accept(new ClassVisitor(Opcodes.ASM9) {
        @Override
        public FieldVisitor visitField(final int access, final String name, final String descriptor, final String signature,
            final Object value) {
          if (PROVIDER.equals(descriptor))
            holders.add(CompiledClasses.keyOf(entry.getKey()));
          return null;
        }
      }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }
    return holders;
  }

  /** The classes that pin: a call of {@code GraphViewPins}, or a lookup through the statement's command context. */
  private static Set<String> pinningClassesOf(final Map<String, byte[]> classes) {
    final Set<String> pinning = new TreeSet<>();
    for (final String site : CompiledClasses.callSitesOf(classes, (owner, name, desc) -> PINS.equals(owner)
        || REGISTRY.equals(owner) && name.startsWith(LOOKUP) && desc.contains(COMMAND_CONTEXT)).keySet())
      pinning.add(site.substring(0, site.indexOf('#')));
    return pinning;
  }

  private static String exemptionOf(final String site) {
    if (UNPINNED_LOOKUPS.containsKey(site))
      return site;
    final String className = site.substring(0, site.indexOf('#'));
    return UNPINNED_LOOKUPS.containsKey(className) ? className : null;
  }
}
