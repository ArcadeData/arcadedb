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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #6602.
 * <p>
 * A chained {@code MATCH} node pattern with no per-row-varying constraint (no label, a multi-label pattern,
 * or a single label with no property/WHERE-driven index lookup) re-opened its candidate scan by calling
 * {@code database.iterateType()} live, on every per-row re-open of
 * {@link com.arcadedb.query.opencypher.executor.steps.MatchNodeStep}'s inner scan (a chained MATCH re-opens
 * that scan once per outer input row - e.g. {@code MATCH (), p0 = ()} re-opens the {@code p0} scan once for
 * every row the {@code ()} pattern produces). Because the step pipeline is pull-based and every step does an
 * internal read-ahead ({@code if (!prevResults.hasNext()) finished = true;} right after its own fill loop) to
 * decide whether it is exhausted, a later re-open of that scan could happen AFTER a downstream {@code MERGE}
 * - logically applied only once all of an earlier MATCH's rows have been produced - had already created a
 * new vertex. That vertex then leaked into the still-in-flight scan as a spurious extra match, retroactively
 * inflating the MATCH clause's own row count after WHERE/MERGE had already started consuming its earlier rows
 * - whether the vertex MERGE created was of a brand-new type or of a type that already existed before the
 * query started.
 * <p>
 * Fixed by materializing the full candidate set once per {@code MatchNodeStep} execution (cached on the step
 * instance, which is rebuilt fresh for every query execution) instead of re-opening a live database cursor on
 * every re-open, whenever the pattern is provably independent of the current input row - see
 * {@code MatchNodeStep#isRowIndependentFullScan}/{@code #cachedFullScanCandidates}. Only applied when the
 * step is chained ({@code prev != null}); a standalone MATCH calls its vertex iterator exactly once
 * regardless, so caching there would only add eager-materialization cost with no correctness benefit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6602MergeAfterEmptyMatchCardinalityTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "UNWIND range(1, 15) AS i CREATE (:Seed {id: i});");
  }

  @Test
  void mergeAfterZeroLengthMatchDoesNotInflateMatchCardinality() {
    // The WHERE filter keeps only alias0 = 'D' ('D' < 'a' lexicographically, 'eUu' + 'eUu' >= 'a'),
    // so the two zero-length MATCH patterns alone must contribute exactly 15 * 15 = 225 rows,
    // regardless of the MERGE clause creating a brand new vertex type partway through evaluation.
    final ResultSet rs = database.command("opencypher", """
        UNWIND ['D', 'eUu'] AS alias0
        MATCH (), p0 = ()
        WHERE NOT ((alias0 + alias0) >= 'a')
        MERGE (:l4 {id: 128})
        RETURN {alias0: alias0, p0: p0} AS row;
        """);

    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());

    assertThat(rows).hasSize(225);

    // Every p0 binding must be one of the 15 pre-existing Seed vertices, seen exactly 15 times each -
    // never the vertex MERGE creates mid-query.
    final Map<Object, Long> p0Counts = rows.stream()
        .map(r -> (Map<?, ?>) r.getProperty("row"))
        .collect(java.util.stream.Collectors.groupingBy(m -> m.get("p0"), java.util.stream.Collectors.counting()));
    assertThat(p0Counts).hasSize(15);
    assertThat(p0Counts.values()).allSatisfy(count -> assertThat(count).isEqualTo(15L));

    // MERGE must still have created exactly one l4 vertex (correct match-or-create semantics).
    assertThat(database.countType("l4", true)).isEqualTo(1L);
  }

  @Test
  void materializedFormAgreesWithDirectReturn() {
    // The workaround from the issue report: forcing materialization via collect()+UNWIND before RETURN.
    // Both forms must return the same row count.
    final ResultSet rs = database.command("opencypher", """
        UNWIND ['D', 'eUu'] AS alias0
        MATCH (), p0 = ()
        WHERE NOT ((alias0 + alias0) >= 'a')
        MERGE (:l4 {id: 128})
        WITH {alias0: alias0, p0: p0} AS row
        WITH collect(row) AS rows
        UNWIND rows AS row
        RETURN row;
        """);

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }

    assertThat(count).isEqualTo(225);
    assertThat(database.countType("l4", true)).isEqualTo(1L);
  }

  @Test
  void mergeCreatingVertexOfAnAlreadyExistingTypeDoesNotInflateMatchCardinalityEither() {
    // Same shape, but MERGE creates a vertex of the type the earlier MATCH clauses are already scanning
    // (:Seed) instead of a brand-new type. Caching only the candidate TYPE list would not have caught this:
    // database.iterateType() still opens a fresh live cursor per re-open even when the type list itself is
    // stable, so the new :Seed vertex is just as capable of leaking into a still-in-flight scan of its own
    // type as a brand-new type is of leaking into a "no label" scan.
    final ResultSet rs = database.command("opencypher", """
        UNWIND ['D', 'eUu'] AS alias0
        MATCH (), p0 = ()
        WHERE NOT ((alias0 + alias0) >= 'a')
        MERGE (:Seed {id: 999})
        RETURN {alias0: alias0, p0: p0} AS row;
        """);

    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());

    assertThat(rows).hasSize(225);
    assertThat(database.countType("Seed", true)).isEqualTo(16L);
  }

  @Test
  void multiLabelPatternWithPropertiesIsStillCacheable() {
    // A multi-label pattern with a property constraint - e.g. (:A:B {flag: true}) - never routes through
    // the single-label index/partition-pruning branches of MatchNodeStep#computeVertexIterator, so unlike a
    // single-label pattern, properties there do not make the scan target row-dependent: they are applied
    // only as a post-fetch filter. This must be just as cacheable/immune to the leak as a label-less pattern.
    database.command("opencypher", "UNWIND range(1, 15) AS i CREATE (:A:B {flag: true, id: i});");

    // Both sides use the SAME multi-label + property pattern (not an anonymous ()) so the pre-existing
    // :Seed vertices from beginTest() - unrelated to this scenario - are not swept into the cartesian too.
    final ResultSet rs = database.command("opencypher", """
        UNWIND ['D', 'eUu'] AS alias0
        MATCH (:A:B {flag: true}), p0 = (:A:B {flag: true})
        WHERE NOT ((alias0 + alias0) >= 'a')
        MERGE (:A:B {flag: true, id: 999})
        RETURN {alias0: alias0, p0: p0} AS row;
        """);

    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());

    // 15 (outer :A:B{flag:true}) * 15 (inner :A:B{flag:true}) = 225, regardless of MERGE creating a 16th
    // matching :A:B {flag:true} vertex partway through evaluation.
    assertThat(rows).hasSize(225);
    assertThat(database.countType("A", true)).isEqualTo(16L);
  }

  @Test
  void singleNonChainedMatchIsUnaffected() {
    // A single MATCH (not cartesian-joined with a second pattern) never re-opens its scan mid-query, so it
    // was never affected by the bug and must keep behaving exactly as before: only alias0 = 'D' survives the
    // WHERE filter, contributing the 15 pre-existing Seed vertices - MERGE's new vertex must not appear.
    final ResultSet rs = database.command("opencypher", """
        UNWIND ['D', 'eUu'] AS alias0
        MATCH (n:Seed)
        WHERE NOT ((alias0 + alias0) >= 'a')
        MERGE (:Seed {id: 999})
        RETURN {alias0: alias0, n: n} AS row;
        """);

    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());

    assertThat(rows).hasSize(15);
    assertThat(database.countType("Seed", true)).isEqualTo(16L);
  }
}
