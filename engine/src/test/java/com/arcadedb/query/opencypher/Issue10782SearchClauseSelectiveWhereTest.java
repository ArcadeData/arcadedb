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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.vector.RecordPredicateRidSet;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.opencypher.procedures.db.DbIndexVectorQueryNodes;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10782: the {@code WHERE} of the Cypher 25 {@code SEARCH} clause was applied to a widening window of the index's
 * nearest candidates, doubled until k rows matched or the window reached {@link VectorUtils#MAX_FETCH_CANDIDATES}. A filter
 * that kept fewer than k of the 100,000 nearest answered short although the index held matching records further away, and
 * every widening reloaded and re-tested the candidates of the pass before. The filter is now the index's allow-list: tested
 * once per record as the walk meets it, the walk expanding until it holds k matches or has met every vector it can reach.
 * <p>
 * Fixture: {@code Doc} vertices {@code d0..d(n-1)} with the EUCLIDEAN vector {@code [i, 1]}, so the neighbors of
 * {@code [0, 1]} are {@code d0, d1, ...} in that order, and the few {@code small} tenant records are the farthest.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10782SearchClauseSelectiveWhereTest extends TestHelper {
  private static final Map<String, Object> Q = Map.of("q", List.of(0f, 1f));

  @Test
  void aWhereMatchingOnlyTheFarthestRecordsAnswersK() {
    createDocs(2_000, 5, "");

    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX `Doc[vector]` FOR $q WHERE d.tenant = 'small' LIMIT 3) "
        + "RETURN d.name AS name")).containsExactly("d1995", "d1996", "d1997");
    // Fewer matches than k: every one of them, nearest first, and no other row
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX `Doc[vector]` FOR $q WHERE d.tenant = 'small' LIMIT 10) "
        + "RETURN d.name AS name")).containsExactly("d1995", "d1996", "d1997", "d1998", "d1999");
  }

  @Test
  void eachRecordIsTestedOncePerSearch() {
    createDocs(2_000, 5, "");

    final List<RID> tested = new ArrayList<>();
    final Predicate<Document> small = record -> {
      tested.add(record.getIdentity());
      return "small".equals(record.getString("tenant"));
    };
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final List<Result> rows = DbIndexVectorQueryNodes.search("Doc[vector]", 3, new float[] { 0f, 1f }, small, context, "test");

    assertThat(rows).extracting(r -> r.<Document>getProperty("node").getString("name"))
        .containsExactly("d1995", "d1996", "d1997");
    // A widening window re-tested the candidates of every earlier pass: 3, 6, 12, ... 2048 of them
    assertThat(tested).as("no record may be tested twice in one search").doesNotHaveDuplicates();
  }

  @Test
  void aSelectiveWalkStopsAtTheStatementsDeadline() {
    createDocs(2_000, 5, "");

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setCommandDeadline(System.currentTimeMillis() + 2_000, "the test deadline");
    final int[] tested = new int[1];
    // The deadline passes while the walk runs, at the 10th record: the walk itself is JVector's and has no check of its
    // own, so without one per record it would test the whole graph and answer as if there were no deadline
    final Predicate<Document> small = record -> {
      if (++tested[0] == 10)
        try {
          Thread.sleep(2_500);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      return "small".equals(record.getString("tenant"));
    };

    assertThatThrownBy(() -> DbIndexVectorQueryNodes.search("Doc[vector]", 3, new float[] { 0f, 1f }, small, context, "test"))
        .isInstanceOf(TimeoutException.class);
    assertThat(tested[0]).as("no record is tested past the deadline").isEqualTo(10);
  }

  @Test
  void aProductQuantizedFilteredWalkStopsAtTheDeadline() {
    createDocs(6_000, 5, ", \"quantization\": \"PRODUCT\", \"pqClusters\": 16");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();
    assertThat(index.isPQSearchAvailable()).as("the PQ path must be the one tested").isTrue();

    // A plain allow-list, not a predicate one, so nothing but the walk's own filter can stop it. Unbounded in size, so the
    // walk is taken rather than the pre-filter plan; the deadline passes on its first membership test
    final WorkGuard guard = WorkGuard.forDeadline("test", System.currentTimeMillis() + 2_000);
    final boolean[] slept = new boolean[1];
    final Set<RID> slowAllowList = new AbstractSet<>() {
      @Override
      public boolean contains(final Object o) {
        if (!slept[0]) {
          slept[0] = true;
          try {
            Thread.sleep(2_500);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        // Admits nothing, so the walk never fills its result queue and keeps going: the guard checks once per 1024 membership
        // tests, and a walk that met fewer ended before it ever looked at the deadline (hence the larger graph)
        return false;
      }

      @Override
      public int size() {
        return Integer.MAX_VALUE;
      }

      @Override
      public Iterator<RID> iterator() {
        throw new UnsupportedOperationException();
      }
    };

    assertThatThrownBy(() -> index.findNeighbors(new float[] { 0f, 1f }, 3, -1, 0, slowAllowList, guard))
        .isInstanceOf(TimeoutException.class);
  }

  @Test
  void aProductQuantizedIndexAnswersEveryFarMatch() {
    createDocs(2_000, 5, ", \"quantization\": \"PRODUCT\", \"pqClusters\": 16");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();
    assertThat(index.isPQSearchAvailable()).as("the PQ path must be the one tested").isTrue();

    final Set<String> small = Set.of("d1995", "d1996", "d1997", "d1998", "d1999");
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // The PQ codes alone: the filtered beam keeps expanding past the nearest records, which all fail the predicate, until it
    // holds k matches or has met the whole graph, so the five farthest are all found
    final Set<RID> pqOnly = RecordPredicateRidSet.matching(r -> "small".equals(r.getString("tenant")), context);
    assertThat(names(index.findNeighbors(new float[] { 0f, 1f }, 10, -1, 0, pqOnly, WorkGuard.forCommandDeadline(null))))
        .containsExactlyInAnyOrderElementsOf(small);

    // Re-ranked on the stored vectors: in their exact order
    final Set<RID> reranked = RecordPredicateRidSet.matching(r -> "small".equals(r.getString("tenant")), context);
    assertThat(names(index.findNeighbors(new float[] { 0f, 1f }, 3, -1, 4, reranked, WorkGuard.forCommandDeadline(null))))
        .containsExactly("d1995", "d1996", "d1997");

    // And through the clause
    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX `Doc[vector]` FOR $q WHERE d.tenant = 'small' LIMIT 10) "
        + "RETURN d.name AS name")).containsExactlyInAnyOrderElementsOf(small);
  }

  /**
   * The issue's case: more records nearer than every match than the widening could ever reach. 100,005 records, the 5 of the
   * {@code small} tenant the farthest: the 100,000 nearest hold none of them.
   */
  @Tag("slow")
  @Test
  void matchesBeyondTheWideningCapAreFound() {
    final int records = VectorUtils.MAX_FETCH_CANDIDATES + 5;
    createDocs(records, 5, "");

    assertThat(names("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX `Doc[vector]` FOR $q WHERE d.tenant = 'small' LIMIT 3) "
        + "RETURN d.name AS name")).containsExactly("d" + (records - 5), "d" + (records - 4), "d" + (records - 3));
  }

  private void createDocs(final int count, final int small, final String extraMetadata) {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.command("sql", "CREATE PROPERTY Doc.tenant STRING");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\"" + extraMetadata + " }");
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newVertex("Doc").set("name", "d" + i, "tenant", i >= count - small ? "small" : "big", "vector",
            new float[] { i, 1f }).save();
    });
  }

  private LSMVectorIndex indexOf() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
  }

  private List<String> names(final String cypher) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher, Q)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  private List<String> names(final List<Pair<RID, Float>> rows) {
    final List<String> names = new ArrayList<>();
    final Set<RID> seen = new HashSet<>();
    for (final Pair<RID, Float> row : rows)
      if (seen.add(row.getFirst()))
        names.add(row.getFirst().asDocument().getString("name"));
    return names;
  }
}
