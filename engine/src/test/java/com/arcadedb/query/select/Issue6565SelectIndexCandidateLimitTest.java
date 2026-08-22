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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/6565
 * <p>
 * {@code SelectExecutor.lookForIndexes()} used to hand the query's RESULT {@code limit} to {@code MultiIndexCursor}
 * as a CANDIDATE cap, applied before {@code evaluateWhere()} and {@code skip} reduce the candidates further downstream.
 * Two shapes were affected: an indexed condition combined (via AND) with a non-indexable condition such as
 * {@code IS NOT NULL} (only one AND child ever keeps its index cursor - see {@code filterWithIndexesFinalNode()} -
 * so the discarded conjunct is still checked by {@code evaluateWhere()} against a candidate stream that was
 * already capped too early), and plain {@code skip + limit} paging over a single indexed equality (the candidate
 * cap must cover {@code skip + limit} records, not just {@code limit}).
 * <p>
 * The fix for those two shapes only caps a candidate stream that a bare indexed leaf (or a synthetic {@code run}
 * wrapper around one) reproduces exactly. Two further consumers can still reduce that stream beyond {@code skip +
 * limit} and had to be accounted for: {@code ORDER BY} whose direction/property doesn't trivially match the forced
 * ascending index scan (forces a full in-memory sort - see {@code isOrderBySafeForCap()}), and an {@code or} of two
 * indexed leaves whose match sets can overlap ({@code MultiIndexCursor} merges children with no RID dedup, so an
 * overlapping record surfaces as two candidates that each burn the cap before downstream dedup runs - see
 * {@code orOfIndexedLeavesIsNeverTreatedAsExactlyIndexed}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6565SelectIndexCandidateLimitTest extends TestHelper {

  private static final int ROWS    = 1000;
  private static final int MATCHES = 200;

  public Issue6565SelectIndexCandidateLimitTest() {
    autoStartTx = true;
  }

  @Override
  protected void beginTest() {
    final var t = database.getSchema().createDocumentType("T");
    t.createProperty("a", Type.STRING);
    t.createProperty("b", Type.STRING);
    t.createProperty("n", Type.INTEGER);
    t.createProperty("g", Type.STRING);
    t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "b");
    t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "n");
    t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "g");

    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        final var d = database.newDocument("T");
        d.set("a", "x");
        d.set("n", i);
        if (i % (ROWS / MATCHES) == 0)
          d.set("b", "set");
        d.set("g", "g" + (i % 5));
        d.save();
      }
    });
  }

  @Test
  void eqAndIsNotNullWithLimitReturnsAllMatchesUpToLimit() {
    // SHAPE A: THE INDEX ONLY COVERS 'a', THE 'b IS NOT NULL' CONJUNCT IS RE-CHECKED BY evaluateWhere() AND MUST NOT
    // BE STARVED BY A CANDIDATE CAP DERIVED FROM 'a' ALONE
    for (final int limit : new int[] { 50, 100, 200, 500, 1000 }) {
      final int expected = Math.min(limit, MATCHES);
      final long count = database.select().fromType("T").where()//
          .property("a").eq().value("x")//
          .and().property("b").isNotNull()//
          .limit(limit).count();
      assertThat(count).as("limit " + limit).isEqualTo(expected);
    }
  }

  @Test
  void eqAndIsNotNullWithNoLimitReturnsAllMatches() {
    final long count = database.select().fromType("T").where()//
        .property("a").eq().value("x")//
        .and().property("b").isNotNull()//
        .count();
    assertThat(count).isEqualTo(MATCHES);
  }

  @Test
  void skipAndLimitOnPlainIndexedEqualityPagesCorrectly() {
    // SHAPE B: ORDINARY PAGING OVER A SINGLE INDEXED EQUALITY. THE CANDIDATE CAP MUST COVER skip + limit.
    assertPage(0, 50, 50);
    assertPage(10, 50, 50);
    assertPage(100, 50, 50);
    assertPage(500, 50, 50);
    assertPage(0, 1000, 1000);
    assertPage(100, 1000, 900);
  }

  @Test
  void countHonoursSkipOnPlainIndexedEquality() {
    final long count = database.select().fromType("T").where()//
        .property("a").eq().value("x")//
        .skip(100).limit(50).count();
    assertThat(count).isEqualTo(50);
  }

  @Test
  void secondPageIsNotEmptyWhenSkipEqualsOrExceedsLimit() {
    // skip >= limit USED TO ALWAYS RETURN ZERO ROWS: THE CANDIDATE CAP WAS limit, SO skip ALONE CONSUMED IT ALL
    final long count = database.select().fromType("T").where()//
        .property("a").eq().value("x")//
        .skip(200).limit(50).count();
    assertThat(count).isEqualTo(50);
  }

  @Test
  void orOfTwoIndexedRangesWithSkipAndLimitPagesCorrectly() {
    // or DISQUALIFIES THE CAP (SEE orOfIndexedLeavesIsNeverTreatedAsExactlyIndexed BELOW), SO THIS PAGES CORRECTLY
    // VIA THE UNCAPPED FALLBACK REGARDLESS OF WHETHER THE TWO BRANCHES OVERLAP - THESE RANGES HAPPEN NOT TO
    final long count = database.select().fromType("T").where()//
        .property("n").lt().value(10)//
        .or().property("n").ge().value(990)//
        .skip(5).limit(10).count();
    assertThat(count).isEqualTo(10);
  }

  @Test
  void orOfIndexedLeavesIsNeverTreatedAsExactlyIndexed() {
    // MultiIndexCursor MERGES ITS CHILDREN WITH NO RID DEDUP: IF THE SAME RECORD SATISFIES BOTH or BRANCHES (SAME-
    // PROPERTY RANGES THAT OVERLAP, OR TWO DIFFERENT PROPERTIES ONE RECORD CAN SATISFY TOGETHER - NOT PROVABLE
    // DISJOINT FROM THE TREE SHAPE ALONE), IT SURFACES AS TWO SEPARATE CANDIDATES, EACH BURNING ONE UNIT OF THE CAP
    // BEFORE THE DOWNSTREAM DEDUP (SelectIterator/executeCount()'s filterOutRecords) EVER SEES IT - SEE
    // overlappingOrBranchesOnTheSamePropertyStillReturnLimitDistinctRows FOR A CONCRETE REPRODUCTION. or MUST
    // THEREFORE STAY CONSERVATIVE EVEN FOR TWO PLAIN INDEXED LEAVES, JUST LIKE and/not.
    final Select select = database.select().fromType("T").where()//
        .property("n").lt().value(10)//
        .or().property("n").ge().value(990)//
        .skip(5).limit(10);
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat(executor.indexCandidateLimit).isEqualTo(-1);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void inOperatorWithSkipAndLimitPagesCorrectly() {
    // THE PER-VALUE CURSORS filterWithIndexesFinalNode() BUILDS FOR AN in_op LEAF ARE THEMSELVES WRAPPED IN A NESTED
    // MultiIndexCursor, WHICH USED TO BE CAPPED WITH THE SAME RAW select.limit (MISSING skip)
    final long count = database.select().fromType("T").where()//
        .property("g").in().value(List.of("g0", "g1", "g2"))//
        .skip(550).limit(100).count();
    assertThat(count).isEqualTo(50);
  }

  @Test
  void bareNonCursorOperatorLeavesIndexCandidateLimitAtMinusOne() {
    // #6577: A BARE neq LEAF IS "INDEXED" PER isTheNodeFullyIndexed()'S LOOSER CHECK, SO soleExactLeaf() RETURNS IT
    // AND A FINITE indexCandidateLimit GETS COMPUTED - BUT filterWithIndexesFinalNode()'S switch NEVER BUILDS A
    // CURSOR FOR neq, SO cursors STAYS EMPTY, lookForIndexes() RETURNS null, AND NO CAP IS EVER ACTUALLY APPLIED. THE
    // TEST-VISIBLE indexCandidateLimit FIELD MUST NOT BE LEFT HOLDING THAT MISLEADING FINITE VALUE.
    final Select select = database.select().fromType("T").where().property("a").neq().value("nonexistent")//
        .limit(50).skip(100);
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      // assertThat(cursor) IS AMBIGUOUS: MultiIndexCursor IMPLEMENTS BOTH Iterator AND Iterable (VIA IndexCursor),
      // AND AssertJ HAS AN OVERLOAD FOR EACH - THE CAST PICKS THE PLAIN Object OVERLOAD INSTEAD
      assertThat((Object) cursor).as("no cursor should have been built for a bare neq leaf").isNull();
      assertThat(executor.indexCandidateLimit).isEqualTo(-1);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void skipPlusLimitOverflowFallsBackToUncapped() {
    // computeExactCandidateLimit()'S long-SUM OVERFLOW GUARD: A skip/limit PAIR THAT OVERFLOWS int MUST FALL BACK
    // TO -1 (UNCAPPED) RATHER THAN WRAP AROUND TO A NEGATIVE-BUT-NOT--1 OR OTHERWISE BOGUS int CAP
    final Select select = database.select().fromType("T").where().property("a").eq().value("x")//
        .limit(20).skip(Integer.MAX_VALUE - 5);
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat(executor.indexCandidateLimit).isEqualTo(-1);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void overlappingOrBranchesOnTheSamePropertyStillReturnLimitDistinctRows() {
    // n > 5 AND n < 500 OVERLAP OVER (5,500): A RECORD IN THAT RANGE IS A CANDIDATE FROM *BOTH* CHILD CURSORS.
    // MultiIndexCursor.next() IS A PLAIN K-WAY MERGE WITH NO RID DEDUP, SO IT EMITS THAT RECORD TWICE; DEDUP ONLY
    // HAPPENS ONE LAYER UP (SelectIterator.fetchNext()/executeCount() VIA filterOutRecords), AFTER THE DUPLICATE HAS
    // ALREADY BEEN COUNTED AGAINST THE CAP. EVERY ONE OF THE 1000 ROWS SATISFIES THIS OR (ONLY n IN [0,5] FAILS THE
    // FIRST BRANCH, ONLY n IN [500,999] FAILS THE SECOND, AND NO ROW FAILS BOTH), SO A CAP EXHAUSTED BY DUPLICATES
    // WOULD RETURN FEWER THAN THE REQUESTED 10.
    final long count = database.select().fromType("T").where()//
        .property("n").gt().value(5)//
        .or().property("n").lt().value(500)//
        .limit(10).count();
    assertThat(count).isEqualTo(10);
  }

  @Test
  void inLeafUnderOrWithSkipAndLimitPagesCorrectly() {
    // AN in_op LEAF NESTED UNDER or PAGES CORRECTLY VIA THE SAME UNCAPPED FALLBACK AS ANY OTHER or.
    // g IN (g0,g1,g2) MATCHES 600 OF THE 1000 ROWS (i % 5 IN {0,1,2}); n = 3 MATCHES EXACTLY 1 ROW WHOSE g IS "g3"
    // (i % 5 == 3), OUTSIDE THE IN SET, SO THE UNION HAS 601 DISTINCT MATCHES.
    final long count = database.select().fromType("T").where()//
        .property("g").in().value(List.of("g0", "g1", "g2"))//
        .or().property("n").eq().value(3)//
        .skip(595).limit(10).count();
    assertThat(count).isEqualTo(6);
  }

  @Test
  void singleBareEqualityCandidateCapCoversSkipPlusLimit() {
    // Select.compile() WRAPS A LONE CONDITION (NO and()/or() CALLED) IN A SYNTHETIC 'run' NODE (Select.setLogic()'S
    // "1ST TIME ONLY" BRANCH), SO THE ROOT'S left IS A SelectTreeNode EVEN THOUGH THE WHOLE TREE IS A SINGLE LEAF.
    // A RESULT-COUNT ASSERTION CANNOT CATCH A REGRESSION HERE: THE LAZY-PULL CONSUMERS ALREADY STOP AT skip + limit
    // EVEN WHEN THE CANDIDATE CAP ITSELF STAYS AT -1, SO THIS CHECKS THE COMPUTED CAP DIRECTLY.
    final Select select = database.select().fromType("T").where().property("a").eq().value("x").limit(50).skip(100);
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat(executor.indexCandidateLimit).isEqualTo(150);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void singleBareInCandidateCapCoversSkipPlusLimit() {
    // SAME GAP AS ABOVE, FOR THE NESTED PER-VALUE CURSOR filterWithIndexesFinalNode() BUILDS FOR A BARE in_op LEAF
    final Select select = database.select().fromType("T").where().property("g").in().value(List.of("g0", "g1", "g2"))//
        .limit(100).skip(550);
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat(executor.indexCandidateLimit).isEqualTo(650);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void skipAndLimitOnPlainIndexedRangePagesCorrectly() {
    // THE "SOLE EXACT LEAF, skip+limit CAPPED CORRECTLY" PATH IS ALSO EXERCISED FOR eq AND in_op ELSEWHERE - ANY
    // OPERATOR filterWithIndexesFinalNode()'S switch TURNS INTO A CURSOR (gt/ge/lt/le/between TOO) IS EQUALLY EXACT,
    // SINCE soleExactLeaf() DOESN'T DISCRIMINATE BY OPERATOR - THIS COVERS IT EXPLICITLY FOR ge.
    // n >= 500 MATCHES 500 OF THE 1000 ROWS (n IN [500,999]).
    final Select select = database.select().fromType("T").where().property("n").ge().value(500)//
        .limit(50).skip(100);
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat(executor.indexCandidateLimit).isEqualTo(150);
    } finally {
      if (cursor != null)
        cursor.close();
    }

    final long count = database.select().fromType("T").where().property("n").ge().value(500)//
        .skip(100).limit(50).count();
    assertThat(count).isEqualTo(50);
  }

  @Test
  void andUnderOrIsNeverTreatedAsExactlyIndexed() {
    // (a = 'x' AND b = 'set') OR n = 5: PRECEDENCE-DRIVEN Select.setLogic() BUILDS A NESTED and NODE UNDER THE or
    // ROOT (and HAS HIGHER PRECEDENCE THAN or). soleExactLeaf() MUST STAY CONSERVATIVE FOR A TREE SHAPED LIKE THIS
    // REGARDLESS OF THE NESTED and, SINCE filterWithIndexesFinalNode() STILL ONLY KEEPS ONE OF THE and'S TWO CHILD
    // CURSORS - THOUGH TODAY THE or ROOT ALONE ALREADY DISQUALIFIES IT BEFORE THE NESTED and IS EVEN CONSIDERED.
    final Select select = database.select().fromType("T").where()//
        .property("a").eq().value("x")//
        .and().property("b").eq().value("set")//
        .or().property("n").eq().value(5)//
        .limit(50).skip(0);
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat(executor.indexCandidateLimit).isEqualTo(-1);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void notLeafIsNeverTreatedAsExactlyIndexed() {
    // SelectOperator.not HAS NO FLUENT ENTRY POINT ON THE Select BUILDER TODAY, BUT THE OPERATOR EXISTS AND
    // soleExactLeaf() MUST STILL TREAT IT CONSERVATIVELY (filterWithIndexesFinalNode() NEVER BUILDS A CURSOR FOR
    // IT), SO THE TREE IS BUILT BY HAND HERE
    final Select select = database.select().fromType("T");
    final SelectTreeNode innerLeaf = new SelectTreeNode(new SelectPropertyValue("a"), SelectOperator.eq, "x");
    select.rootTreeElement = new SelectTreeNode(innerLeaf, SelectOperator.not, null);
    select.limit(50).skip(0);

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat(executor.indexCandidateLimit).isEqualTo(-1);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  @Test
  void orderByDescendingOnAscendingScannedIndexWithLimitReturnsTrueTail() {
    // SelectIterator.fetchResultInCaseOfOrderBy() MATERIALIZES BY DRAINING THE FULL ITERATOR WHENEVER THE REQUESTED
    // ORDER DOESN'T TRIVIALLY MATCH THE (ALWAYS ASCENDING) INDEX SCAN filterWithIndexesFinalNode() PERFORMS - A
    // CANDIDATE CAP WOULD STOP THAT DRAIN AT skip + limit, LETTING THE IN-MEMORY SORT SEE ONLY THE FIRST FEW
    // ASCENDING CANDIDATES INSTEAD OF EVERY MATCH. A DESCENDING orderBy() ON THE SAME PROPERTY THE WHERE CLAUSE
    // INDEXES MUST DISABLE THE CAP TOO.
    final List<Document> list = database.select().fromType("T").where()//
        .property("n").gt().value(-1)//
        .orderBy("n", false).limit(10).documents().toList();
    assertThat(list).hasSize(10);
    for (int i = 0; i < list.size(); i++)
      assertThat(list.get(i).getInteger("n")).isEqualTo(ROWS - 1 - i);
  }

  @Test
  void orderByAscendingMatchingScanDirectionStillEngagesCap() {
    // THE ORDER BY SAFETY CHECK MUST NOT BE ALL-OR-NOTHING: WHEN THE REQUESTED ORDER TRIVIALLY MATCHES THE
    // (ALWAYS ASCENDING) INDEX SCAN ON THE SAME PROPERTY - SelectIterator.fetchResultInCaseOfOrderBy()'S OWN
    // TRIVIAL-MATCH CHECK - THE CAP CAN AND SHOULD STILL ENGAGE
    final Select select = database.select().fromType("T").where().property("n").gt().value(-1)//
        .orderBy("n", true).limit(50).skip(100);
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    final MultiIndexCursor cursor = executor.lookForIndexes();
    try {
      assertThat(executor.indexCandidateLimit).isEqualTo(150);
    } finally {
      if (cursor != null)
        cursor.close();
    }
  }

  private void assertPage(final int skip, final int limit, final int expected) {
    final long count = database.select().fromType("T").where()//
        .property("a").eq().value("x")//
        .skip(skip).limit(limit).count();
    assertThat(count).as("skip " + skip + " limit " + limit).isEqualTo(expected);
  }
}
