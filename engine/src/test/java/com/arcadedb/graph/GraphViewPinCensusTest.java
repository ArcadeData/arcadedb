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
package com.arcadedb.graph;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level guard for issue #10415 (and #10393): every read of a graph view goes through a pin.
 * <p>
 * A view publishes a base numbered afresh on every rebuild, so a caller that asks it several questions - a node id, then the
 * neighbours, then their RIDs - must ask them all of one snapshot, or a rebuild in between answers the later ones about other
 * vertices, without failing. The pin is per caller: {@code GraphTraversalProviderRegistry.findProvider(Database, ...)}
 * hands out the live view, because the planner caches what it gets and must not cache a snapshot. A call site that forgets to
 * pin compiles and runs, and is wrong only while a rebuild races it. A behavioural test cannot hold a rule for a call site that
 * does not exist yet; reading the sources can:
 * <ul>
 *   <li>every lookup of a view either passes the statement's command context, which returns the statement's pin, or is wrapped
 *   in {@link GraphViewPins}, unless it is listed here with the reason it reads no more than once or is the planner's;</li>
 *   <li>every query operator or step that holds a view in a field pins it when it executes, unless listed here.</li>
 * </ul>
 * A new call site is therefore a decision, not an accident.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphViewPinCensusTest {
  private static final List<Path> SOURCES = List.of(Path.of("src/main/java"), Path.of("../gremlin/src/main/java"));

  private static final Pattern LOOKUP          = Pattern.compile("GraphTraversalProviderRegistry\\.findProvider\\w*\\(");
  private static final Pattern CONTEXT_LOOKUP  = Pattern.compile("findProvider\\w*\\(\\s*(context|ctx)\\b");
  private static final Pattern PROVIDER_FIELD  = Pattern.compile(
      "^\\s*(private|protected|public)?\\s*(final\\s+)?GraphTraversalProvider\\s+\\w+\\s*(=[^;]*)?;", Pattern.MULTILINE);
  private static final Pattern OPERATOR_OR_STEP = Pattern.compile("extends\\s+(AbstractPhysicalOperator|AbstractExecutionStep)\\b");

  /** Lookups that need no pin, as {@code <file>#<text of the statement>}, with the reason. */
  private static final Map<String, String> UNPINNED_LOOKUPS = Map.of(
      "CypherOptimizer.java#", "the planner: its plans are cached, so they hold the live view and the operators pin it when they run",
      "StatisticsProvider.java#", "the planner's estimates: one count per lookup, read once",
      "MatchExecutionPlanner.java#", "the SQL planner: MatchGAVFusedStep pins the view when it runs",
      "CypherExecutionPlan.java#findProviderWithoutWaiting(database, type)", "the planner's row estimate: one countAllEdges() per lookup",
      "ShortestPathFinder.java#", "captures one snapshot of the view itself (currentSnapshot()) and reads only through it",
      "AbstractAlgoProcedure.java#findProviderWithoutWaiting(db, relTypes)",
      "findReadyProvider(): its caller findProvider(Database, String[], CommandContext) pins what it returns");

  /** Operators and steps holding a view in a field without pinning it themselves, with the reason. */
  private static final Map<String, String> UNPINNED_HOLDERS = Map.of(
      "GAVOneHopScanStep.java", "built for one execution by CypherExecutionPlan.tryGAVOneHopScan(), with the statement's pin");

  @Test
  void everyLookupOfAViewIsPinned() {
    final List<String> unpinned = new ArrayList<>();
    forEachSource((file, source) -> {
      if (file.getFileName().toString().equals("GraphTraversalProviderRegistry.java"))
        return;
      final Matcher lookup = LOOKUP.matcher(source);
      while (lookup.find()) {
        final String statement = statementAround(source, lookup.start());
        if (CONTEXT_LOOKUP.matcher(statement).find() || statement.contains("GraphViewPins.") || statement.contains("pins.pin("))
          continue;
        if (!isExempt(UNPINNED_LOOKUPS, file, statement))
          unpinned.add(file.getFileName() + ": " + statement.strip().replaceAll("\\s+", " "));
      }
    });

    assertThat(unpinned).as("a view looked up without a pin is read in whatever snapshot it serves at each call, and a rebuild "
        + "between two calls answers the later ones about other vertices (issue #10415). To resolve: pass the statement's "
        + "CommandContext (findProvider(context, ...)), or wrap the lookup in GraphViewPins.pin(context, ...) / "
        + "GraphViewPins.pinAlone(...); if the lookup reads the view once or belongs to the planner, add it to UNPINNED_LOOKUPS "
        + "with the reason").isEmpty();
  }

  @Test
  void everyOperatorHoldingAViewPinsItWhenItRuns() {
    final List<String> unpinned = new ArrayList<>();
    final List<String> holders = new ArrayList<>();
    forEachSource((file, source) -> {
      if (!OPERATOR_OR_STEP.matcher(source).find() || !PROVIDER_FIELD.matcher(source).find())
        return;
      holders.add(file.getFileName().toString());
      if (!source.contains("GraphViewPins") && !CONTEXT_LOOKUP.matcher(source).find()
          && !UNPINNED_HOLDERS.containsKey(file.getFileName().toString()))
        unpinned.add(file.getFileName().toString());
    });

    assertThat(holders).as("the scan found the operators that hold a view").contains("GAVExpandAll.java", "GAVExpandInto.java",
        "GAVFusedChainOperator.java", "MatchGAVFusedStep.java", "MatchRelationshipStep.java");
    assertThat(unpinned).as("a plan is cached and holds the live view: an operator that reads it without pinning it reads a "
        + "different snapshot at each call (issue #10415). To resolve: read the view through GraphViewPins.pin(context, provider) "
        + "when the operator executes; if it is built for one execution with a pinned view, add it to UNPINNED_HOLDERS with the "
        + "reason").isEmpty();
  }

  @Test
  void theExemptionsAreNotStale() {
    final Set<String> usedLookups = new TreeSet<>();
    final Set<String> holders = new TreeSet<>();
    forEachSource((file, source) -> {
      final Matcher lookup = LOOKUP.matcher(source);
      while (lookup.find())
        for (final String key : UNPINNED_LOOKUPS.keySet())
          if (matches(key, file, statementAround(source, lookup.start())))
            usedLookups.add(key);
      if (PROVIDER_FIELD.matcher(source).find())
        holders.add(file.getFileName().toString());
    });
    assertThat(usedLookups).as("every exempted lookup still exists").containsExactlyInAnyOrderElementsOf(UNPINNED_LOOKUPS.keySet());
    assertThat(holders).as("every exempted holder still holds a view").containsAll(UNPINNED_HOLDERS.keySet());
  }

  private static boolean isExempt(final Map<String, String> exemptions, final Path file, final String statement) {
    for (final String key : exemptions.keySet())
      if (matches(key, file, statement))
        return true;
    return false;
  }

  private static boolean matches(final String key, final Path file, final String statement) {
    final int hash = key.indexOf('#');
    return file.getFileName().toString().equals(key.substring(0, hash)) && statement.contains(key.substring(hash + 1));
  }

  /** The statement {@code position} is in: from the end of the previous one, or of the block it opens, to its {@code ;}. */
  private static String statementAround(final String source, final int position) {
    final int start = Math.max(source.lastIndexOf(';', position), Math.max(source.lastIndexOf('{', position),
        source.lastIndexOf('}', position))) + 1;
    final int end = source.indexOf(';', position);
    return source.substring(start, end < 0 ? source.length() : end);
  }

  private interface SourceVisitor {
    void visit(Path file, String source);
  }

  private static void forEachSource(final SourceVisitor visitor) {
    for (final Path root : SOURCES) {
      assertThat(root).as("the census reads the sources of " + root).isDirectory();
      try (final Stream<Path> files = Files.walk(root)) {
        files.filter(f -> f.toString().endsWith(".java")).forEach(f -> visitor.visit(f, withoutComments(read(f))));
      } catch (final IOException e) {
        throw new IllegalStateException("Cannot list the sources of " + root, e);
      }
    }
  }

  /** The source without its comments, which name the lookups they explain. */
  private static String withoutComments(final String source) {
    return source.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("//[^\\n]*", "");
  }

  private static String read(final Path file) {
    try {
      return Files.readString(file, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new IllegalStateException("Cannot read " + file, e);
    }
  }
}
