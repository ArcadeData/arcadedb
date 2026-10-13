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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9912: a MATCH node with both {@code type:} and {@code rid:} must answer the record at that RID only when it is of
 * that type (polymorphically). The prefetch and the single-node root fetched by RID and dropped the type; the edge-pattern
 * root scanned the type and dropped the RID, so every record of the type became a root once the type was too large to be
 * prefetched.
 * <p>
 * {@code Small} has fewer records than the MATCH prefetch threshold, {@code Big} more, so both root paths are covered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9912MatchRidWithTypeTest extends TestHelper {
  private static final int BIG_COUNT = 250;

  private RID small2;
  private RID smallSub;
  private RID big7;
  private RID other0;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Small");
      database.command("sql", "CREATE VERTEX TYPE SmallSub EXTENDS Small");
      database.command("sql", "CREATE VERTEX TYPE Big");
      database.command("sql", "CREATE VERTEX TYPE Other");
      database.command("sql", "CREATE EDGE TYPE R");

      for (int i = 0; i < 5; i++)
        database.command("sql", "CREATE VERTEX Small SET n = " + i);
      database.command("sql", "CREATE VERTEX SmallSub SET n = 100");
      for (int i = 0; i < BIG_COUNT; i++)
        database.command("sql", "CREATE VERTEX Big SET n = " + i);
      for (int i = 0; i < 3; i++)
        database.command("sql", "CREATE VERTEX Other SET n = " + i);

      // EVERY Other POINTS TO EVERY Small, SmallSub AND Big: A ROOT THAT DROPS ITS rid: SEES ALL OF THEM AS NEIGHBOURS
      database.command("sql", "CREATE EDGE R FROM (SELECT FROM Other) TO (SELECT FROM Small)");
      database.command("sql", "CREATE EDGE R FROM (SELECT FROM Other) TO (SELECT FROM Big)");
    });

    small2 = ridOf("Small", 2);
    smallSub = ridOf("SmallSub", 100);
    big7 = ridOf("Big", 7);
    other0 = ridOf("Other", 0);
  }

  @Test
  void singleNodeRidOfAnotherTypeAnswersNoRows() {
    assertThat(column("MATCH {type:Other, as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).isEmpty();
    assertThat(column("MATCH {type:Other, as:x, rid:" + big7 + "} RETURN x.n AS n", "n")).isEmpty();
  }

  @Test
  void singleNodeRidOfTheDeclaredTypeMatches() {
    assertThat(column("MATCH {type:Small, as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).containsExactly(2);
    assertThat(column("MATCH {type:Big, as:x, rid:" + big7 + "} RETURN x.n AS n", "n")).containsExactly(7);
  }

  @Test
  void singleNodeRidOfASubtypeMatches() {
    assertThat(column("MATCH {type:Small, as:x, rid:" + smallSub + "} RETURN x.n AS n", "n")).containsExactly(100);
    // AND NOT THE OTHER WAY AROUND: A Small IS NOT A SmallSub
    assertThat(column("MATCH {type:SmallSub, as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).isEmpty();
  }

  @Test
  void edgeRootOnASmallTypeHonoursTheType() {
    // Small IS PREFETCHED: THE PREFETCH USED TO FETCH THE RID AND DROP THE TYPE
    assertThat(column("MATCH {type:Small, as:s, rid:" + small2 + "}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
    assertThat(column("MATCH {type:Other, as:s, rid:" + small2 + "}.in('R'){as:o} RETURN o.n AS n", "n")).isEmpty();
  }

  @Test
  void edgeRootOnABigTypeHonoursTheRid() {
    // Big IS NOT PREFETCHED: THE ROOT USED TO SCAN THE TYPE AND DROP THE RID, ANSWERING 3 NEIGHBOURS FOR EVERY Big
    assertThat(column("MATCH {type:Big, as:b, rid:" + big7 + "}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
    assertThat(column("MATCH {type:Small, as:b, rid:" + big7 + "}.in('R'){as:o} RETURN o.n AS n", "n")).isEmpty();
  }

  @Test
  void edgeRootRidOfASubtypeMatches() {
    assertThat(column("MATCH {type:Small, as:s, rid:" + smallSub + "}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
  }

  @Test
  void parameterizedRidKeepsTheTypeCheck() {
    final String query = "MATCH {type:Big, as:b, rid: :rid}.in('R'){as:o} RETURN o.n AS n ORDER BY n";
    assertThat(column(query, "n", Map.of("rid", big7))).containsExactly(0, 1, 2);
    // SAME TEXT, SO THE CACHED PLAN: THE TYPE IS CHECKED ON THE RECORD, NOT DECIDED FOR THE FIRST BINDING
    assertThat(column(query, "n", Map.of("rid", small2))).isEmpty();
    assertThat(column(query, "n", Map.of("rid", big7))).containsExactly(0, 1, 2);
  }

  @Test
  void ridWithABucketOfAnotherTypeAnswersNoRows() {
    final String smallBucket = database.getSchema().getType("Small").getBuckets(false).getFirst().getName();
    final String bigBucket = database.getSchema().getType("Big").getBuckets(false).getFirst().getName();
    assertThat(column("MATCH {bucket:" + smallBucket + ", as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).containsExactly(2);
    assertThat(column("MATCH {bucket:" + bigBucket + ", as:x, rid:" + small2 + "} RETURN x.n AS n", "n")).isEmpty();
    assertThat(column("MATCH {bucket:" + bigBucket + ", as:b, rid:" + big7 + "}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
    assertThat(column("MATCH {bucket:" + smallBucket + ", as:b, rid:" + big7 + "}.in('R'){as:o} RETURN o.n AS n", "n")).isEmpty();
  }

  @Test
  void ridTypeAndWhereAreAllApplied() {
    // THE TYPE CHECK IS CHAINED AFTER THE where: OF THE SELECT BY RID: BOTH MUST STILL SEE (AND PASS ON) THE RECORD ITSELF
    for (final String root : new String[] { "{type:Big, as:b, rid:%s, where:(n = %d)}", "{type:Small, as:b, rid:%s, where:(n = %d)}" }) {
      final boolean big = root.contains("Big");
      assertThat(column("MATCH " + root.formatted(big7, 7) + " RETURN b.n AS n", "n")).isEqualTo(big ? List.of(7) : List.of());
      assertThat(column("MATCH " + root.formatted(big7, 8) + " RETURN b.n AS n", "n")).isEmpty();
      assertThat(column("MATCH " + root.formatted(big7, 7) + ".in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
          .isEqualTo(big ? List.of(0, 1, 2) : List.of());
      assertThat(column("MATCH " + root.formatted(big7, 8) + ".in('R'){as:o} RETURN o.n AS n", "n")).isEmpty();
    }
  }

  @Test
  void typeAndBucketWithoutRidAreBothApplied() {
    // THE SAME RULE WITHOUT A rid: THE ROOT SCANNED THE TYPE AND DROPPED THE bucket:, WHILE A TRAVERSAL CHECKED BOTH
    final String smallBucket = database.getSchema().getType("Small").getBuckets(false).getFirst().getName();
    final String subBucket = database.getSchema().getType("SmallSub").getBuckets(false).getFirst().getName();
    assertThat(column("MATCH {type:Small, bucket:" + subBucket + ", as:x} RETURN x.n AS n", "n")).containsExactly(100);
    assertThat(column("MATCH {type:Small, bucket:" + smallBucket + ", as:x} RETURN x.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2, 3, 4);
    assertThat(column("MATCH {type:Small, bucket:" + subBucket + ", as:x}.in('R'){as:o} RETURN o.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2);
    final String bigBucket = database.getSchema().getType("Big").getBuckets(false).getFirst().getName();
    assertThat(column("MATCH {type:Big, bucket:" + bigBucket + ", as:x}.in('R'){as:o} RETURN count(*) AS c", "c"))
        .containsExactly(3L * BIG_COUNT);
  }

  @Test
  void aRidReadingAnotherAliasIsNeverTheRoot() {
    // THE RID IS ONLY KNOWN ONCE o IS BOUND: AS A ROOT OR A PREFETCH IT WOULD BE RESOLVED AGAINST NOTHING
    database.transaction(() -> database.command("sql", "UPDATE Other SET target = " + small2));
    for (final String type : new String[] { "", "type:Small, " })
      assertThat(column("MATCH {type:Other, as:o}.out('R'){" + type + "as:s, rid:{\"@rid\": $matched.o.target}} "
          + "RETURN o.n AS n ORDER BY n", "n")).containsExactly(0, 1, 2);
  }

  @Test
  void unknownTypeOrBucketNextToARidIsStillReported() {
    assertThatThrownBy(() -> column("MATCH {type:NoSuchType, as:x, rid:" + small2 + "} RETURN x.n AS n", "n"))//
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("Type 'NoSuchType' not defined");
    assertThatThrownBy(() -> column("MATCH {bucket:NoSuchBucket, as:x, rid:" + small2 + "} RETURN x.n AS n", "n"))//
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("Bucket 'NoSuchBucket' not defined");
  }

  @Test
  void aRidBoundNodeIsTheRootEvenWhenAnotherNodeIsDeclaredFirst() {
    // THE RID USED TO BE ESTIMATED AS THE WHOLE TYPE, SO THE PLANNER STARTED FROM Other AND CHECKED THE RID ON EVERY NEIGHBOUR
    final String query = "MATCH {type:Other, as:o}.out('R'){type:Big, as:b, rid:" + big7 + "} RETURN o.n AS n ORDER BY n";
    assertThat(column(query, "n")).containsExactly(0, 1, 2);
    final String plan = database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(plan).contains("+ MATCH      <----\n  {o}.out('R'){b}");

    // A RID-ONLY NODE (NO type:) IS A ROOT CANDIDATE TOO
    final String ridOnly = "MATCH {type:Other, as:o}.out('R'){as:b, rid:" + big7 + "} RETURN o.n AS n ORDER BY n";
    assertThat(column(ridOnly, "n")).containsExactly(0, 1, 2);
    assertThat(database.query("sql", "EXPLAIN " + ridOnly).getExecutionPlan().get().prettyPrint(0, 2))
        .contains("+ MATCH      <----\n  {o}.out('R'){b}");
  }

  @Test
  void ridRootStillWorksWithoutType() {
    assertThat(column("MATCH {as:x, rid:" + other0 + "}.out('R'){type:Small, as:s} RETURN s.n AS n ORDER BY n", "n"))
        .containsExactly(0, 1, 2, 3, 4, 100);
  }

  private RID ridOf(final String type, final int n) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE n = ?", n)) {
      return rs.next().getIdentity().get();
    }
  }

  private List<Object> column(final String query, final String column) {
    return column(query, column, Map.of());
  }

  private List<Object> column(final String query, final String column, final Map<String, Object> params) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, params)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }
}
