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
import org.objectweb.asm.Opcodes;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backstop for issue #6318 part 2: {@code Issue6302AlgoGraphDrivenWorkGuardTest}'s parameterised table is the only thing that
 * notices an {@code algo.*} procedure shipping without a {@code WorkGuard} / {@code WorkCheckpoint}, and only if somebody
 * remembers to add a row for it. This census finds every procedure, so a new one is caught automatically rather than by memory.
 * <p>
 * The judgement call it cannot make mechanically is whether a procedure's dominant loop is actually superlinear in the graph
 * (needs a guard) or a single O(V + E) pass (does not - "one pass costs what loading the graph and emitting the rows already
 * cost", per {@code Issue6302AlgoGraphDrivenWorkGuardTest}). That judgement is recorded once, here, as {@link #SINGLE_PASS_EXEMPT}:
 * every procedure must either use a {@code WorkGuard} in its own code or be listed with a one-line reason.
 * <p>
 * The census ranges over the authorities (#11288), in the compiled classes of every module ({@link CompiledClasses}): a
 * procedure is a concrete class whose super types reach {@code AbstractAlgoProcedure}, and it is guarded when its own code -
 * a method, a lambda, a nested or anonymous class - uses a {@code WorkGuard}: checks one, or obtains one to hand to a
 * checkpointed kernel. The source-text census it replaces took a procedure to be an {@code Algo*.java} file of one package
 * directory, so a procedure named otherwise or written in another package was out of range, and it took a guard to be a
 * variable named {@code guard}. What neither can tell is whether the guard is checked in the DOMINANT loop: that stays the
 * reviewer's question, as it was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoProcedureGuardCensusTest {
  private static final String PROCEDURE_BASE = "com/arcadedb/query/opencypher/procedures/algo/AbstractAlgoProcedure";
  private static final String GUARD          = "com/arcadedb/query/sql/executor/WorkGuard";
  private static final String ALGO           = "query/opencypher/procedures/algo/";

  /**
   * Procedures whose dominant work is a single O(V + E) pass (each vertex visited once, each edge read off that vertex's own
   * adjacency - never a second sweep of all vertices per vertex), a knob-bounded computation with no graph-driven multiplier,
   * or a query against a caller-supplied pair rather than the whole graph. Verified by reading each class's {@code execute()}
   * at the time it was listed (issue #6318); a procedure whose shape changes to something superlinear must move off this list
   * and gain a guard, not the other way round.
   */
  private static final Map<String, String> SINGLE_PASS_EXEMPT = new TreeMap<>(Map.ofEntries(
      Map.entry(ALGO + "AlgoAdamicAdar", "one node's own adjacency plus each neighbour's own adjacency: O(deg + sum of neighbour degrees), not the graph"),
      Map.entry(ALGO + "AlgoArticulationPoints", "Tarjan-style single DFS, O(V + E)"),
      Map.entry(ALGO + "AlgoAssortativity", "two O(V + E) passes: degree computation, then one edge-endpoint-degree sum"),
      Map.entry(ALGO + "AlgoAStar", "single priority-queue search bounded by the path found, not the whole graph"),
      Map.entry(ALGO + "AlgoBFS", "single breadth-first traversal, O(V + E)"),
      Map.entry(ALGO + "AlgoBiconnectedComponents", "DFS forest over unvisited starts, each vertex/edge visited once overall, O(V + E)"),
      Map.entry(ALGO + "AlgoBipartiteCheck", "single two-colouring BFS/DFS, O(V + E)"),
      Map.entry(ALGO + "AlgoBridges", "single low-link DFS, O(V + E)"),
      Map.entry(ALGO + "AlgoCommonNeighbors", "one node's own adjacency plus one other node's own adjacency: O(deg(a) + deg(b))"),
      Map.entry(ALGO + "AlgoConductance", "a handful of O(V) / O(V + E) passes over precomputed degree and community arrays"),
      Map.entry(ALGO + "AlgoCycleDetection", "single DFS with a recursion-stack colour array, O(V + E)"),
      Map.entry(ALGO + "AlgoDegreeCentrality", "one degree lookup per node, O(V) (or O(1) per node CSR-backed, issue #6316)"),
      Map.entry(ALGO + "AlgoDFS", "single depth-first traversal, O(V + E)"),
      Map.entry(ALGO + "AlgoDijkstra", "single priority-queue shortest path, O((V + E) log V)"),
      Map.entry(ALGO + "AlgoDijkstraSingleSource", "single priority-queue search from one source, O((V + E) log V)"),
      Map.entry(ALGO + "AlgoGraphColoring", "single greedy pass, one node's own adjacency scanned once each, O(V + E)"),
      Map.entry(ALGO + "AlgoGraphSummary", "a handful of O(V) / O(V + E) aggregate passes over the loaded graph"),
      Map.entry(ALGO + "AlgoJaccardSimilarity", "one node's own adjacency plus one other node's own adjacency: O(deg(a) + deg(b))"),
      Map.entry(ALGO + "AlgoKCore", "bucket-queue peeling, each vertex removed once and each edge relaxed once, O(V + E)"),
      Map.entry(ALGO + "AlgoLongestPathDAG", "topological order plus one relaxation pass over each vertex's own adjacency, O(V + E)"),
      Map.entry(ALGO + "AlgoModularityScore", "a handful of O(V) / O(V + E) passes over precomputed degree and community arrays"),
      Map.entry(ALGO + "AlgoPreferentialAttachment", "O(1) product of two precomputed degrees"),
      Map.entry(ALGO + "AlgoResourceAllocation", "one node's own adjacency plus one other node's own adjacency: O(deg(a) + deg(b))"),
      Map.entry(ALGO + "AlgoSameCommunity", "O(1) lookup of two precomputed community labels"),
      Map.entry(ALGO + "AlgoSCC", "single Tarjan/Kosaraju pass, O(V + E)"),
      Map.entry(ALGO + "AlgoTopologicalSort", "single Kahn's-algorithm pass, O(V + E)"),
      Map.entry(ALGO + "AlgoTotalNeighbors", "one node's own adjacency plus one other node's own adjacency: O(deg(a) + deg(b))"),
      Map.entry(ALGO + "AlgoWCC", "single union-find pass over every edge, O(V + E)")));

  @Test
  void everyAlgoProcedureIsEitherGuardedOrAJustifiedSinglePassExemption() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> procedures = proceduresOf(classes);
    assertThat(procedures).as("the census must find the procedures").hasSizeGreaterThan(50).contains(ALGO + "AlgoPageRank");

    final Set<String> guarded = guardedClassesOf(classes);
    final Set<String> unguardedAndUnlisted = new TreeSet<>();
    final Set<String> stale = new TreeSet<>(SINGLE_PASS_EXEMPT.keySet());
    for (final String procedure : procedures)
      if (guarded.contains(procedure))
        continue;
      else if (SINGLE_PASS_EXEMPT.containsKey(procedure))
        stale.remove(procedure);
      else
        unguardedAndUnlisted.add(procedure);

    assertThat(unguardedAndUnlisted).as("every algo.* procedure must either use a WorkGuard in its own code or be added to SINGLE_PASS_EXEMPT "
        + "with a one-line reason its dominant loop is not superlinear in the graph (issue #6318)").isEmpty();
    assertThat(stale).as("SINGLE_PASS_EXEMPT entries that no longer apply: the procedure is gone, or now reaches a guard").isEmpty();
  }

  /** The concrete classes whose super types reach the procedure base, as {@link CompiledClasses} keys them. */
  private static Set<String> proceduresOf(final Map<String, byte[]> classes) {
    final Set<String> hierarchy = CompiledClasses.implementationsOf(PROCEDURE_BASE, classes);
    final Set<String> procedures = new TreeSet<>();
    for (final Map.Entry<String, byte[]> entry : classes.entrySet()) {
      final ClassReader reader = new ClassReader(entry.getValue());
      if (hierarchy.contains(reader.getClassName()) && (reader.getAccess() & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_INTERFACE)) == 0)
        procedures.add(CompiledClasses.keyOf(entry.getKey()));
    }
    return procedures;
  }

  /**
   * The top-level classes, as keyed, whose own code - a method, a lambda, a nested or anonymous class - uses a guard: calls a
   * method of {@code WorkGuard} ({@code check}, {@code checkPeriodically}, or hands one on as {@code guard::check}), or obtains
   * one ({@code newWorkGuard(context)}, any call answering a {@code WorkGuard}) to hand to the kernel that checks it. Only the
   * procedure's own code: the base class's helpers check while they load the graph, and following a call into them would
   * count every procedure as guarded whatever its dominant loop does.
   */
  private static Set<String> guardedClassesOf(final Map<String, byte[]> classes) {
    final Set<String> guarded = new TreeSet<>();
    for (final String site : CompiledClasses.callSitesOf(classes,
        (owner, name, desc) -> GUARD.equals(owner) || desc.endsWith(")L" + GUARD + ";")).keySet()) {
      final String className = site.substring(0, site.indexOf('#'));
      final int nested = className.indexOf('$');
      guarded.add(nested < 0 ? className : className.substring(0, nested));
    }
    return guarded;
  }
}
