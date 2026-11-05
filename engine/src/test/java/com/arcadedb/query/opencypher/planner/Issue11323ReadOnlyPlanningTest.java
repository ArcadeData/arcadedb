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
package com.arcadedb.query.opencypher.planner;

import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11323: the openCypher indexed point lookup {@code MATCH (n:V {id: $id}) RETURN n.id} got about 14% slower in 26.11.1.
 * Bisected to the per-clause property-read walk of {@link CypherEagernessAnalyzer#observeClause} (#11005, #11173, #11230), about
 * 1.2 KB per execution: the plan is built on every execution, and the walk ran for a statement that writes nothing, where no
 * {@code needsBarrierForPropertyWrites} question is ever asked. The analyzer of a read-only statement now collects nothing.
 * <p>
 * The write side is pinned by the tests of those issues; this pins the gate, both ways.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11323ReadOnlyPlanningTest {
  private static final Cypher25AntlrParser PARSER = new Cypher25AntlrParser();

  // A projection that reads n.k and a MATCH that filters on it: both are what a later SET n.k would conflict with
  private static final String READS = "MATCH (n:V) WHERE n.k = 1 WITH n, n.k AS old RETURN old";

  @Test
  void theAnalyzerOfAReadOnlyStatementCollectsNoPropertyRead() {
    final CypherStatement readOnly = PARSER.parse(READS);
    assertThat(readOnly.isReadOnly()).isTrue();

    final CypherEagernessAnalyzer analyzer = CypherEagernessAnalyzer.forStatement(readOnly);
    observeEveryClause(analyzer, readOnly);

    assertThat(analyzer.hasPendingRead()).isFalse();
    assertThat(analyzer.needsBarrierForPropertyWrites(setOf("MATCH (n) SET n.k = 2"))).isFalse();

    // Not vacuous: the very same clauses, observed by an analyzer that tracks them, conflict with that SET
    final CypherEagernessAnalyzer tracking = new CypherEagernessAnalyzer();
    observeEveryClause(tracking, readOnly);
    assertThat(tracking.needsBarrierForPropertyWrites(setOf("MATCH (n) SET n.k = 2"))).isTrue();
  }

  @Test
  void theAnalyzerOfAWritingStatementStillWeighsItsWrites() {
    // The same reads, in a statement that writes: the analyzer collects them, and the SET of their key conflicts
    final CypherStatement writing = PARSER.parse(READS.replace(" RETURN old", " SET n.k = old + 1"));
    assertThat(writing.isReadOnly()).isFalse();

    final CypherEagernessAnalyzer analyzer = CypherEagernessAnalyzer.forStatement(writing);
    observeEveryClause(analyzer, writing);

    assertThat(analyzer.needsBarrierForPropertyWrites(setOf("MATCH (n) SET n.k = 2"))).isTrue();

    // An analyzer built without a statement keeps weighing every write, as before
    final CypherEagernessAnalyzer unscoped = new CypherEagernessAnalyzer();
    observeEveryClause(unscoped, writing);
    assertThat(unscoped.needsBarrierForPropertyWrites(setOf("MATCH (n) SET n.k = 2"))).isTrue();
  }

  /** Every clause observed as not starting the statement: no variable is known to bind one entity per row. */
  private static void observeEveryClause(final CypherEagernessAnalyzer analyzer, final CypherStatement statement) {
    for (final ClauseEntry entry : statement.getClausesInOrder())
      analyzer.observeClause(entry, false, Set.of());
  }

  private static SetClause setOf(final String query) {
    for (final ClauseEntry entry : PARSER.parse(query).getClausesInOrder())
      if (entry.getType() == ClauseEntry.ClauseType.SET)
        return entry.getTypedClause();
    throw new IllegalArgumentException("no SET in " + query);
  }
}
