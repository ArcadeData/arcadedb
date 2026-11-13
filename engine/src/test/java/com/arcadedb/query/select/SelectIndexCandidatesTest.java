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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectIndexCandidatesTest {

  /**
   * Regression for #6565: the result limit was used as a candidate cap before the residual filter and skip.
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
  @Nested
  class CandidateLimit extends TestHelper {

    private static final int ROWS    = 1000;
    private static final int MATCHES = 200;

    public CandidateLimit() {
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
    void skipAndLimitOnPlainIndexedBetweenPagesCorrectly() {
      // CODE REVIEW GAP: between HAS A DISTINCT TWO-BOUND CURSOR SHAPE (node.index.range(...) WITH BOTH BOUNDS SET)
      // VS THE OPEN-ENDED gt/ge/lt/le RANGES ALREADY COVERED - WORTH ITS OWN CASE RATHER THAN RELYING ON THE SHARED
      // soleExactLeaf()/filterWithIndexesFinalNode() CLASSIFICATION BEING CORRECT BY INSPECTION.
      // n BETWEEN 400 AND 449 MATCHES 50 OF THE 1000 ROWS (n IN [400,449]).
      final Select select = database.select().fromType("T").where().property("n").between().values(400, 449)//
          .limit(20).skip(10);
      select.compile();

      final SelectExecutor executor = new SelectExecutor(select);
      final MultiIndexCursor cursor = executor.lookForIndexes();
      try {
        assertThat(executor.indexCandidateLimit).isEqualTo(30);
      } finally {
        if (cursor != null)
          cursor.close();
      }

      final long count = database.select().fromType("T").where().property("n").between().values(400, 449)//
          .skip(10).limit(20).count();
      assertThat(count).isEqualTo(20);
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

  /**
   * Regression for #6577: an OR with an operator that builds no index cursor dropped records.
   * https://github.com/ArcadeData/arcadedb/issues/6577
   * <p>
   * {@code SelectExecutor.isTheNodeFullyIndexed()} (used by {@code filterWithIndexesFinalNode()}'s "under an OR, both
   * sides must be indexed" gate) only excluded {@code is_null}/{@code is_not_null} from "this leaf is indexed" -
   * every other operator on an indexed property, including {@code neq}/{@code like}/{@code ilike}, was treated as
   * indexed as long as the property itself had an index. But {@code filterWithIndexesFinalNode()}'s cursor-building
   * switch only knows how to build a cursor for {@code eq}/{@code in_op}/{@code between}/{@code gt}/{@code ge}/
   * {@code lt}/{@code le} - a {@code neq}/{@code like}/{@code ilike} leaf silently contributed zero cursors while
   * still passing the "both sides indexed" gate for its {@code or} sibling. The result: for
   * {@code a != 'x' OR n = -1}, the {@code eq} leaf's cursor alone became the WHOLE candidate stream, and every
   * record matching only the {@code neq} branch was silently dropped - not "evaluated less efficiently", genuinely
   * missing from the result.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OrWithNonCursorBuildableOperator extends TestHelper {

    private static final int ROWS = 1000;

    public OrWithNonCursorBuildableOperator() {
      autoStartTx = true;
    }

    @Override
    protected void beginTest() {
      final var t = database.getSchema().createDocumentType("T");
      t.createProperty("a", Type.STRING);
      t.createProperty("n", Type.INTEGER);
      t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
      t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "n");

      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newDocument("T").set("a", "x", "n", i).save();
      });
    }

    @Test
    void orWithNeqOnOneSideMustNotDropTheOtherBranch() {
      // EVERY ROW HAS a = "x" != "nonexistent", SO THE neq BRANCH ALONE ALREADY MATCHES ALL ROWS
      final long count = database.select().fromType("T").where()//
          .property("a").neq().value("nonexistent")//
          .or().property("n").eq().value(-1).count();

      assertThat(count).isEqualTo(ROWS);
    }

    @Test
    void orWithNeqOnOneSideMustNotDropTheOtherBranchReversed() {
      // SAME SHAPE, neq ON THE RIGHT-HAND SIDE OF THE or INSTEAD OF THE LEFT
      final long count = database.select().fromType("T").where()//
          .property("n").eq().value(-1)//
          .or().property("a").neq().value("nonexistent").count();

      assertThat(count).isEqualTo(ROWS);
    }

    @Test
    void orWithLikeOnOneSideMustNotDropTheOtherBranch() {
      final long count = database.select().fromType("T").where()//
          .property("a").like().value("x")//
          .or().property("n").eq().value(-1).count();

      assertThat(count).isEqualTo(ROWS);
    }

    @Test
    void orWithIlikeOnOneSideMustNotDropTheOtherBranch() {
      final long count = database.select().fromType("T").where()//
          .property("a").ilike().value("X")//
          .or().property("n").eq().value(-1).count();

      assertThat(count).isEqualTo(ROWS);
    }

    @Test
    void orWithNeqOnBothSidesFallsBackToFullScanCorrectly() {
      // NEITHER SIDE IS CURSOR-BUILDABLE, SO THIS MUST FALL ALL THE WAY BACK TO A FULL SCAN - STILL CORRECT
      final long count = database.select().fromType("T").where()//
          .property("a").neq().value("nonexistent")//
          .or().property("n").neq().value(-999999).count();

      assertThat(count).isEqualTo(ROWS);
    }
  }

  /**
   * Regression for #8048: an index cursor under a nested OR hid the records of the other branch.
   * https://github.com/ArcadeData/arcadedb/issues/8048
   * <p>
   * The union of the cursors {@code SelectExecutor.filterWithIndexesFinalNode()} builds is the ONLY set of candidate
   * records a native select ever evaluates, so a cursor is sound only when every record that could satisfy the WHERE
   * is guaranteed to be inside it. Under an {@code or} that means both sides have to be index-answerable, and the
   * method did check that - but only against the leaf's IMMEDIATE sibling. Two ways out of it, both reproduced here:
   * <ol>
   *   <li>A leaf nested one level deeper. In {@code b = 1 and a = 0 or b = 2} the tree is
   *   {@code or(and(b=1, a=0), b=2)}, the indexed leaf {@code a = 0} has the {@code and} as its parent, the
   *   {@code parent.operator == or} test was false and the cursor was built unconditionally. The candidate set
   *   collapsed to "records with a = 0" and every record matching {@code b = 2} was never looked at.</li>
   *   <li>A composite {@code or} sibling. {@code isTheNodeFullyIndexed()} answered {@code left || right} for an
   *   {@code or} node, where an OR branch is only index-answerable when BOTH of its sides are.</li>
   * </ol>
   * The invariant under test is the one this whole code path exists to preserve: an index is a PERFORMANCE decision,
   * so the same select over the same data must return the same rows with and without one. Each case is therefore run
   * twice against the identical SQL - once with an {@code LSM_TREE} index on {@code a} and once with none - and the
   * un-indexed run is the control that was always correct.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IndexCursorUnderNestedOr extends TestHelper {

    private static final int CARDINALITY = 6;

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType("D");
      type.createProperty("a", Type.INTEGER);
      type.createProperty("b", Type.INTEGER);

      database.transaction(() -> {
        for (int a = 0; a < CARDINALITY; a++)
          for (int b = 0; b < CARDINALITY; b++) {
            final MutableDocument doc = database.newDocument("D");
            doc.set("a", a);
            doc.set("b", b);
            doc.save();
          }
      });
    }

    @Test
    void indexedLeafNestedUnderAnAndInsideAnOrDoesNotDropTheOtherBranch() {
      // b = 1 and a = 0 or b = 2 -> or(and(b=1, a=0), b=2)
      runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
          .property("b").eq().value(1)//
          .and().property("a").eq().value(0)//
          .or().property("b").eq().value(2).count(), "b = 1 and a = 0 or b = 2");
    }

    @Test
    void indexedLeafFirstInsideTheAndBranchDoesNotDropTheOtherBranch() {
      runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
          .property("a").eq().value(0)//
          .and().property("b").eq().value(1)//
          .or().property("b").eq().value(2).count(), "a = 0 and b = 1 or b = 2");
    }

    @Test
    void indexedLeafInTheAndBranchOnTheRightOfTheOrDoesNotDropTheOtherBranch() {
      runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
          .property("b").eq().value(2)//
          .or().property("a").eq().value(0)//
          .and().property("b").eq().value(1).count(), "b = 2 or a = 0 and b = 1");
    }

    @Test
    void chainedOrsWithAnUnindexedTailDoNotDropIt() {
      // a = 1 or a = 3 or b = 2: THE LEFT-NESTED or MEANS THE FIRST TWO LEAVES' IMMEDIATE SIBLING IS INDEXED, WHILE
      // THE OUTER or'S OTHER BRANCH - b = 2 - IS NOT. ONLY A PATH WALK SEES THAT
      runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
          .property("a").eq().value(1)//
          .or().property("a").eq().value(3)//
          .or().property("b").eq().value(2).count(), "a = 1 or a = 3 or b = 2");
    }

    @Test
    void rightNestedOrSiblingIsOnlyIndexedWhenBothOfItsSidesAre() {
      // a = 1 OR (b = 1 OR a = 2). THE FLUENT BUILDER CANNOT PRODUCE A RIGHT-NESTED or, BUT Select.json() CAN.
      // isTheNodeFullyIndexed()'S or ARM USED TO ANSWER left || right, SO THE INNER or CALLED ITSELF FULLY INDEXED ON
      // THE STRENGTH OF a = 2 ALONE AND THE OUTER a = 1 LEAF GOT ITS CURSOR.
      // #8167: UNTIL THAT FIX THE JSON BELOW DID NOT ACTUALLY COMPILE TO THE RIGHT-NESTED SHAPE THIS COMMENT NAMES -
      // Select.json(JSONObject) REPLAYED THE NESTED ARRAY'S LEAVES THROUGH THE PRECEDENCE MACHINE AND PRODUCED THE
      // LEFT-NESTED (a = 1 or b = 1) or a = 2. THE TEST STAYED VALID EITHER WAY, SINCE THE LEFT-NESTED SIBLING
      // EXERCISES THE SAME || TO && CHANGE, BUT IT NOW TESTS THE TREE IT CLAIMS TO
      final JSONObject json = new JSONObject(
          "{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1],\"or\",[[\":b\",\"=\",1],\"or\",[\":a\",\"=\",2]]]}");

      runWithAndWithoutIndex(() -> database.select().json(json).count(), "a = 1 or (b = 1 or a = 2)");
    }

    @Test
    void plainOrWithOneIndexedSideStaysCorrect() {
      // THE CONTROL FROM THE ISSUE: HERE THE LEAF'S PARENT *IS* THE or, THE PRE-EXISTING GATE ALREADY FIRED AND THE
      // ANSWER WAS ALWAYS RIGHT. IT MUST STAY RIGHT
      runWithAndWithoutIndex(() -> database.select().fromType("D").where()//
          .property("a").eq().value(0)//
          .or().property("b").eq().value(2).count(), "a = 0 or b = 2");
    }

    @Test
    void anIndexIsStillUsedWhenBothOrBranchesAreIndexed() {
      // THE FIX MUST NOT DISABLE THE INDEX PATH ALTOGETHER: WITH BOTH PROPERTIES INDEXED, BOTH BRANCHES ARE
      // ANSWERABLE AND TWO CURSORS MUST STILL BE BUILT
      database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
      database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "b");
      try {
        final Select select = database.select().fromType("D");
        select.where().property("a").eq().value(0).or().property("b").eq().value(2).compile();

        final SelectExecutor executor = new SelectExecutor(select);
        try (var cursor = executor.lookForIndexes()) {
          assertThat((Object) cursor).isNotNull();
          assertThat(executor.metrics().get("usedIndexes")).isEqualTo(2);
        }

        assertThat(database.select().fromType("D").where()//
            .property("a").eq().value(0)//
            .or().property("b").eq().value(2).count()).isEqualTo(sqlCount("a = 0 or b = 2"));
      } finally {
        dropIndexesOn("a");
        dropIndexesOn("b");
      }
    }

    /**
     * Runs {@code nativeCount} against the identical SQL twice: once with an index on {@code a} and once without.
     * Both answers have to match SQL, which also makes them match each other - the invariant an index must never
     * break.
     */
    private void runWithAndWithoutIndex(final java.util.function.LongSupplier nativeCount, final String sqlWhere) {
      final long expected = sqlCount(sqlWhere);

      assertThat(nativeCount.getAsLong()).as("without an index on 'a': " + sqlWhere).isEqualTo(expected);

      database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
      try {
        assertThat(nativeCount.getAsLong()).as("with an index on 'a': " + sqlWhere).isEqualTo(expected);
      } finally {
        dropIndexesOn("a");
      }
    }

    private void dropIndexesOn(final String property) {
      for (final var index : database.getSchema().getType("D").getIndexesByProperties(property))
        database.getSchema().dropIndex(index.getName());
    }

    private long sqlCount(final String where) {
      return ((Number) database.query("sql", "SELECT count(*) AS c FROM D WHERE " + where).nextIfAvailable().getProperty("c"))
          .longValue();
    }
  }

  /**
   * Regression for #8153: a merge starting with an equality cursor crashed with ArrayIndexOutOfBoundsException.
   * https://github.com/ArcadeData/arcadedb/issues/8153
   * <p>
   * {@link MultiIndexCursor} sampled the key types and the comparator it merges with from the FIRST child cursor. An
   * equality lookup ({@code TypeIndex.get()}) answered that question with an empty key-type array and a null comparator,
   * so a WHERE starting with an {@code =} leaf followed by two range leaves on the same indexed property crashed the
   * k-way merge with an {@link ArrayIndexOutOfBoundsException} instead of answering.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class KeylessFirstCursor extends TestHelper {

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType("T");
      type.createProperty("b", Type.INTEGER);
      database.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          final MutableDocument doc = database.newDocument("T");
          doc.set("b", i);
          doc.save();
        }
      });
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "b");
    }

    @Test
    void equalityLeafFirstFollowedByTwoRangeLeaves() {
      assertThat(database.select().json(new JSONObject(
              "{\"fromType\":\"T\",\"where\":[[[\":b\",\"=\",1],\"and\",[\":b\",\">=\",1]],\"and\",[\":b\",\"<=\",1]]}"))
          .count()).isEqualTo(1);

      assertThat(database.select().fromType("T").where().property("b").eq().value(1).and().property("b").ge().value(1).and()
          .property("b").le().value(1).count()).isEqualTo(1);
    }

    @Test
    void sameLeavesInAnotherOrderAgree() {
      assertThat(database.select().fromType("T").where().property("b").ge().value(1).and().property("b").le().value(1).and()
          .property("b").eq().value(1).count()).isEqualTo(1);
      assertThat(database.select().fromType("T").where().property("b").eq().value(1).or().property("b").ge().value(3).or()
          .property("b").le().value(0).count()).isEqualTo(4);
    }

    @Test
    void orAcrossIndexesOfDifferentKeyTypesIsAUnionNotAKeyMerge() {
      // THE LEAVES OF AN or CAN BE ANSWERED BY DIFFERENT INDEXES, WHOSE KEYS ARE NOT COMPARABLE: MERGING THEM BY KEY
      // COMPARED AN INTEGER UNDER A STRING KEY TYPE AS SOON AS THREE CURSORS WITH REAL KEYS WERE LIVE
      final DocumentType type = database.getSchema().getType("T");
      type.createProperty("s", Type.STRING);
      database.transaction(() -> database.command("sql", "UPDATE T SET s = 'v' + b"));
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "s");

      assertThat(database.select().fromType("T").where().property("b").ge().value(3).or().property("s").le().value("v0").or()
          .property("b").eq().value(1).count()).isEqualTo(4);
      assertThat(database.select().fromType("T").where().property("s").eq().value("v2").or().property("b").ge().value(4).or()
          .property("s").ge().value("v3").count()).isEqualTo(3);
    }

    @Test
    void inLeafServesAnAscendingOrderByInKeyOrder() {
      // A NESTED in_op CURSOR MERGES EQUALITY LOOKUPS OVER ONE INDEX, AND WITH A SINGLE USED INDEX SelectIterator SKIPS
      // ITS IN-MEMORY SORT: THE LOOKUPS USED TO CARRY NO KEY, SO THEY CAME BACK IN THE ORDER OF THE IN LIST
      final List<Integer> values = new ArrayList<>();
      database.select().fromType("T").where().property("b").in().value(List.of(4, 0, 2)).orderBy("b", true).documents()
          .forEachRemaining(d -> values.add(d.getInteger("b")));
      assertThat(values).containsExactly(0, 2, 4);
    }

    @Test
    void equalityLookupCarriesTheIndexKeyMetadata() {
      final TypeIndex index = (TypeIndex) database.getSchema().getType("T").getIndexesByProperties("b").getFirst();
      final IndexCursor cursor = index.get(new Object[] { 1 });
      assertThat(cursor.getBinaryKeyTypes()).isEqualTo(index.getBinaryKeyTypes());
      assertThat(cursor.getComparator()).isNotNull();
      assertThat(cursor.getKeys()).containsExactly(1);
    }

    @Test
    void sqlIndexLookupProjectsTheKeyItMatched() {
      // FetchFromIndexStep PROJECTS cursor.getKeys(): AN EQUALITY LOOKUP ON A TYPE INDEX USED TO ANSWER key = []
      final String indexName = database.getSchema().getType("T").getIndexesByProperties("b").getFirst().getName();
      try (final ResultSet rs = database.query("sql", "SELECT FROM index:`" + indexName + "` WHERE key = 1")) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("key")).isEqualTo(new Object[] { 1 });
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void equalityLookupHonoursItsLimit() {
      // THE PER-BUCKET LIMIT USED TO BE size - limit, NEGATIVE, SO EVERY BUCKET WAS READ IN FULL; AND A LIMIT OF 0 ADDED
      // THE FIRST ENTRY BEFORE CHECKING THE BOUND
      database.transaction(() -> {
        for (int i = 0; i < 20; i++)
          database.newDocument("T").set("b", 7).save();
      });
      final TypeIndex index = (TypeIndex) database.getSchema().getType("T").getIndexesByProperties("b").getFirst();

      assertThat(count(index.get(new Object[] { 7 }, 0))).isZero();
      assertThat(count(index.get(new Object[] { 7 }, 3))).isEqualTo(3);
      assertThat(count(index.get(new Object[] { 7 }, -1))).isEqualTo(20);
    }

    private static int count(final IndexCursor cursor) {
      int n = 0;
      while (cursor.hasNext()) {
        cursor.next();
        ++n;
      }
      return n;
    }

    @Test
    void mergeSamplesKeyMetadataFromTheFirstChildThatHasIt() {
      final TypeIndex index = (TypeIndex) database.getSchema().getType("T").getIndexesByProperties("b").getFirst();

      final List<IndexCursor> children = new ArrayList<>();
      children.add(new EmptyIndexCursor());
      children.add(index.range(true, new Object[] { 3 }, true, null, false));
      children.add(index.range(true, null, false, new Object[] { 1 }, true));

      final List<Object> keys = new ArrayList<>();
      try (final MultiIndexCursor merged = new MultiIndexCursor(children, -1, true)) {
        assertThat(merged.getBinaryKeyTypes()).isEqualTo(index.getBinaryKeyTypes());
        assertThat(merged.getComparator()).isNotNull();
        while (merged.hasNext()) {
          final Identifiable next = merged.next();
          assertThat(next).isNotNull();
          keys.add(merged.getKeys()[0]);
        }
      }
      assertThat(keys).containsExactly(0, 1, 3, 4);
    }
  }

  /**
   * Regression for #9613 and #9614: case-folded and repeated IN candidates answered the wrong rows.
   * The native {@code select()} must answer what the unindexed type answers, whatever index serves it.
   * <ul>
   *   <li>#9613: a {@code COLLATE ci} index answers an equality with every record that differs from the operand only in case,
   *   which the residual filter then discards. The candidate cap of #6565 stopped after {@code skip + limit} of those, so the
   *   records that matched were never reached: {@code .limit(1)} answered nothing.</li>
   *   <li>#9614: an {@code IN} list probed the index once per item, repeated items included, and the merge of the probes
   *   returned each record once per probe that found it.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CollatedAndRepeatedCandidates extends TestHelper {

    private void load(final String type, final String index, final String[][] rows) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".id STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".t STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".n INTEGER");
      if (index != null)
        for (final String statement : index.replace("%T", type).split(";"))
          database.command("sql", statement);
      database.transaction(() -> {
        for (final String[] row : rows) {
          final var doc = database.newDocument(type).set("id", row[0]).set("s", row[1]);
          if (row.length > 2)
            doc.set("t", row[2]);
          if (row.length > 3)
            doc.set("n", Integer.parseInt(row[3]));
          doc.save();
        }
      });
    }

    private static List<String> ids(final SelectWhereAfterBlock query) {
      return query.documents().toList().stream().map((final Document d) -> d.getString("id")).sorted().toList();
    }

    private static List<String> ids(final Select query) {
      return query.documents().toList().stream().map((final Document d) -> d.getString("id")).sorted().toList();
    }

    private SelectWhereLeftBlock where(final String type) {
      return database.select().fromType(type).where();
    }

    // ---------------------------------------------------------------- #9613

    private static final String[][] CASED = { { "s1", "Smith" }, { "x1", "SMITH" }, { "x2", "smith" }, { "s2", "Smith" },
        { "x3", "sMITH" }, { "x4", "SmitH" } };

    @Test
    void limitOnAFoldedIndexReachesTheMatchesBehindTheFoldedOnes() {
      load("Ci", "CREATE INDEX ON %T (s COLLATE ci) NOTUNIQUE", CASED);
      load("Plain", null, CASED);

      assertThat(ids(where("Ci").property("s").eq().value("Smith"))).containsExactly("s1", "s2");
      for (int limit = 1; limit <= 6; limit++) {
        final int expected = Math.min(limit, 2);
        assertThat(where("Plain").property("s").eq().value("Smith").limit(limit).documents().toList()).hasSize(expected);
        assertThat(where("Ci").property("s").eq().value("Smith").limit(limit).documents().toList()).as("limit " + limit)
            .hasSize(expected);
        assertThat(where("Ci").property("s").eq().value("Smith").limit(limit).count()).as("count limit " + limit)
            .isEqualTo(expected);
        assertThat(where("Ci").property("s").in().value(List.of("Smith")).limit(limit).documents().toList())
            .as("in limit " + limit).hasSize(expected);
      }

      // paging: the two pages hold the two matches, one each
      final List<String> pages = new ArrayList<>();
      pages.addAll(ids(where("Ci").property("s").eq().value("Smith").skip(0).limit(1)));
      pages.addAll(ids(where("Ci").property("s").eq().value("Smith").skip(1).limit(1)));
      assertThat(pages).containsExactlyInAnyOrder("s1", "s2");
    }

    @Test
    void limitOnAFoldedCompositeIndexReachesTheMatchesBehindTheFoldedOnes() {
      final String[][] rows = { { "s1", "Smith", "UK" }, { "x1", "SMITH", "uk" }, { "x2", "smith", "UK" }, { "s2", "Smith", "UK" },
          { "x3", "Smith", "Uk" } };
      load("Ci", "CREATE INDEX ON %T (s COLLATE ci, t COLLATE ci) NOTUNIQUE", rows);
      load("Plain", null, rows);

      for (int limit = 1; limit <= 5; limit++) {
        final int expected = Math.min(limit, 2);
        assertThat(where("Plain").property("s").eq().value("Smith").and().property("t").eq().value("UK").limit(limit).documents()
            .toList()).hasSize(expected);
        assertThat(where("Ci").property("s").eq().value("Smith").and().property("t").eq().value("UK").limit(limit).documents()
            .toList()).as("limit " + limit).hasSize(expected);
        assertThat(where("Ci").property("s").eq().value("Smith").and().property("t").eq().value("UK").limit(limit).count())
            .as("count limit " + limit).isEqualTo(expected);
      }
    }

    @Test
    void limitOnACaseSensitiveIndexStillCapsTheCandidates() {
      // the cap stays armed where the probe is exact: the cursor pulls skip + limit candidates and no more
      load("Cs", "CREATE INDEX ON %T (s) NOTUNIQUE", CASED);
      load("Ci", "CREATE INDEX ON %T (s COLLATE ci) NOTUNIQUE", CASED);
      assertThat(candidateLimit("Cs")).isEqualTo(1);
      // ... and is never armed over folded keys
      assertThat(candidateLimit("Ci")).isEqualTo(-1);
    }

    private int candidateLimit(final String type) {
      final Select select = where(type).property("s").eq().value("Smith").limit(1);
      select.compile();
      final SelectExecutor executor = new SelectExecutor(select);
      executor.lookForIndexes().close();
      return executor.indexCandidateLimit;
    }

    // ---------------------------------------------------------------- #9614

    private static final String[][] LETTERS = { { "a1", "a", "p", "1" }, { "a2", "a", "q", "1" }, { "b1", "b", "p", "2" },
        { "b2", "b", "r", "2" }, { "c1", "c", "r", "3" } };

    @Test
    void repeatedValueInAnInListReturnsEachRecordOnce() {
      load("Idx", "CREATE INDEX ON %T (s) NOTUNIQUE;CREATE INDEX ON %T (t) NOTUNIQUE;CREATE INDEX ON %T (id) UNIQUE", LETTERS);
      load("Plain", null, LETTERS);

      for (final List<String> list : List.of(List.of("a"), List.of("a", "a"), List.of("a", "a", "b"), List.of("a", "b", "c", "a"),
          List.of("a", "a", "a"))) {
        final List<String> expected = ids(where("Plain").property("s").in().value(list));
        assertThat(ids(where("Idx").property("s").in().value(list))).as("IN " + list).isEqualTo(expected);
        assertThat(where("Idx").property("s").in().value(list).count()).as("count IN " + list).isEqualTo(expected.size());
        assertThat(where("Idx").property("s").in().value(list).limit(2).documents().toList()).as("limit IN " + list)
            .hasSize(Math.min(2, expected.size()));
      }

      // a UNIQUE index and the same key twice: one record, once
      assertThat(ids(where("Idx").property("id").in().value(List.of("a1", "a1")))).containsExactly("a1");

      // a sibling that contributes nothing must not change the answer
      assertThat(ids(where("Idx").property("s").in().value(List.of("a", "a")).or().property("t").eq().value("zz")))//
          .isEqualTo(ids(where("Plain").property("s").in().value(List.of("a", "a")).or().property("t").eq().value("zz")));
      assertThat(ids(where("Idx").property("s").in().value(List.of("a", "a")).and().property("t").eq().value("q")))//
          .containsExactly("a2");

      // the SQL control agrees
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Idx WHERE s IN ['a', 'a']")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
      }
    }

    @Test
    void valuesThatAreOneKeyToTheIndexReturnEachRecordOnce() {
      // different values the index converts or folds onto one key: 1 and 1L and "1" on an INTEGER key, 'A' and 'a' on a folded one
      load("Num", "CREATE INDEX ON %T (n) NOTUNIQUE", LETTERS);
      final List<Object> numbers = List.of(1, 1L, "1");
      assertThat(ids(where("Num").property("n").in().value(numbers))).containsExactly("a1", "a2");
      assertThat(where("Num").property("n").in().value(numbers).count()).isEqualTo(2);
      assertThat(where("Num").property("n").in().value(numbers).limit(2).documents().toList()).hasSize(2);
      // a mixed list is merged in key order instead of raising a ClassCastException, a value the key type cannot read matches
      // nothing, and the answer is the unindexed one
      load("PlainNum", null, LETTERS);
      final List<Object> mixed = List.of("2", 1, "abc", 3L);
      assertThat(ids(where("Num").property("n").in().value(mixed))).isEqualTo(ids(where("PlainNum").property("n").in().value(mixed)))
          .contains("a1", "a2", "c1");
      assertThat(where("Num").property("n").in().value(List.of("abc")).count()).isZero();

      load("Hash", "CREATE INDEX ON %T (n) NOTUNIQUE_HASH", LETTERS);
      assertThat(ids(where("Hash").property("n").in().value(List.of(1, 1L, "1", 2)))).containsExactly("a1", "a2", "b1", "b2");

      load("Ci", "CREATE INDEX ON %T (s COLLATE ci) NOTUNIQUE", LETTERS);
      // the residual filter is case sensitive: 'A' matches nothing, 'a' its two records, once each
      assertThat(ids(where("Ci").property("s").in().value(List.of("A", "a")))).containsExactly("a1", "a2");
      assertThat(where("Ci").property("s").in().value(List.of("A", "a")).count()).isEqualTo(2);
    }

    @Test
    void decimalsOfOneValueInTwoScalesAreOneKey() {
      // 1.0 and 1.00 differ by equals() but are one key: probed once, each record returned once, on both index kinds
      for (final String kind : List.of("NOTUNIQUE", "NOTUNIQUE_HASH")) {
        final String type = "Dec" + kind.replace("_", "");
        database.command("sql", "CREATE DOCUMENT TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".d DECIMAL");
        database.command("sql", "CREATE INDEX ON " + type + " (d) " + kind);
        database.transaction(() -> {
          database.newDocument(type).set("id", "x1").set("d", new BigDecimal("1.0")).save();
          database.newDocument(type).set("id", "x2").set("d", new BigDecimal("1")).save();
          database.newDocument(type).set("id", "y1").set("d", new BigDecimal("2.5")).save();
        });
        final List<Object> values = List.of(new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("1"));
        assertThat(ids(where(type).property("d").in().value(values))).as(kind).containsExactly("x1", "x2");
        assertThat(where(type).property("d").in().value(values).count()).as(kind).isEqualTo(2);
        assertThat(where(type).property("d").in().value(values).limit(2).documents().toList()).as(kind).hasSize(2);
      }
    }

    @Test
    void zeroAndNegativeZeroAreOneKey() {
      database.command("sql", "CREATE DOCUMENT TYPE Dbl");
      database.command("sql", "CREATE PROPERTY Dbl.d DOUBLE");
      database.command("sql", "CREATE INDEX ON Dbl (d) NOTUNIQUE");
      database.transaction(() -> {
        database.newDocument("Dbl").set("id", "z1").set("d", 0.0d).save();
        database.newDocument("Dbl").set("id", "z2").set("d", -0.0d).save();
        database.newDocument("Dbl").set("id", "o1").set("d", 1.0d).save();
      });
      final List<Object> values = List.of(0.0d, -0.0d);
      assertThat(ids(where("Dbl").property("d").in().value(values))).containsExactly("z1", "z2");
      assertThat(where("Dbl").property("d").in().value(values).count()).isEqualTo(2);
    }

    @Test
    void anUnreadableDateDoesNotProbeTheNullKey() {
      // a DATETIME key reads 'abc' as null: the rows without a date must not answer it, with or without a limit
      database.command("sql", "CREATE DOCUMENT TYPE Dt");
      database.command("sql", "CREATE PROPERTY Dt.t DATETIME");
      database.command("sql", "CREATE INDEX ON Dt (t) NOTUNIQUE NULL_STRATEGY INDEX");
      final LocalDateTime when = LocalDateTime.of(2026, 1, 1, 10, 0);
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          database.newDocument("Dt").set("id", "n" + i).save();
        database.newDocument("Dt").set("id", "d1").set("t", when).save();
      });
      final List<Object> values = List.of("abc", when);
      assertThat(ids(where("Dt").property("t").in().value(values))).containsExactly("d1");
      assertThat(ids(where("Dt").property("t").in().value(values).limit(1))).containsExactly("d1");
      assertThat(where("Dt").property("t").in().value(List.of("abc")).count()).isZero();
    }

    @Test
    void emptyAndUnreadableInListsMatchNothingOnEveryIndexKind() {
      load("Lsm", "CREATE INDEX ON %T (n) NOTUNIQUE", LETTERS);
      load("Hash", "CREATE INDEX ON %T (n) NOTUNIQUE_HASH", LETTERS);
      for (final String type : List.of("Lsm", "Hash")) {
        assertThat(where(type).property("n").in().value(List.of()).count()).as(type + " IN []").isZero();
        assertThat(where(type).property("n").in().value(List.of()).documents().toList()).as(type + " IN []").isEmpty();
        assertThat(where(type).property("n").in().value(List.of("abc", "x1")).count()).as(type + " unreadable").isZero();
        assertThat(where(type).property("n").in().value(List.of("abc", "x1")).limit(1).documents().toList()).as(type + " unreadable")
            .isEmpty();
        // an unreadable value next to readable ones only drops itself
        assertThat(ids(where(type).property("n").in().value(List.of("abc", 3)))).as(type + " mixed").containsExactly("c1");
      }
    }

    @Test
    void bytesKeysThatAreEqualReturnEachRecordOnce() {
      // a byte[] key has an identity equals(), so two equal arrays are two probes of one key: the merge returns each record once,
      // and a duplicate does not count against the limit
      for (final String type : List.of("BinIdx", "BinPlain")) {
        database.getSchema().createDocumentType(type).createProperty("b", Type.BINARY);
        database.transaction(() -> {
          database.newDocument(type).set("id", "x1").set("b", new byte[] { 1, 2 }).save();
          database.newDocument(type).set("id", "x2").set("b", new byte[] { 1, 2 }).save();
          database.newDocument(type).set("id", "y1").set("b", new byte[] { 3 }).save();
        });
      }
      database.getSchema().getType("BinIdx").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "b");

      final List<Object> values = List.of(new byte[] { 1, 2 }, new byte[] { 1, 2 }, new byte[] { 3 });
      final List<String> expected = ids(where("BinPlain").property("b").in().value(values));
      assertThat(expected).containsExactly("x1", "x2", "y1");
      assertThat(ids(where("BinIdx").property("b").in().value(values))).isEqualTo(expected);
      assertThat(where("BinIdx").property("b").in().value(values).count()).isEqualTo(3);
      assertThat(where("BinIdx").property("b").in().value(values).limit(3).documents().toList()).hasSize(3);
    }
  }

  /**
   * Regression for #8937: ORDER BY was elided over an index holding folded keys.
   * Regression test for issue #8937: {@code database.select()} elided its ORDER BY whenever the single index used matched the
   * order property, but a {@code COLLATE ci} index holds folded keys, so its iteration order is not the order of the values and
   * {@code orderBy + limit} returned the wrong rows.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OrderByFoldedIndex extends TestHelper {

    private static final String[] VALUES = { "AZb", "AZ", "azc", "Az", "AY", "B", "a[", "abc", "Mzz", "m", "Zoo", "apple", "Banana" };

    private void load(final String type, final boolean collateCi) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
      if (collateCi)
        database.command("sql", "CREATE INDEX ON " + type + " (s COLLATE ci) NOTUNIQUE");
      else
        database.command("sql", "CREATE INDEX ON " + type + " (s) NOTUNIQUE");
      database.transaction(() -> {
        for (final String v : VALUES)
          database.newDocument(type).set("s", v).save();
      });
    }

    private List<String> ordered(final String type, final boolean asc, final int limit) {
      final Select q = database.select().fromType(type).where().property("s").gt().value("A").orderBy("s", asc);
      return (limit > 0 ? q.limit(limit) : q).documents().toList().stream().map(d -> d.getString("s")).toList();
    }

    @Test
    void limitedOrderByOnAFoldedIndexMatchesTheUnindexedAnswer() {
      load("Ci", true);
      load("Nx", false);
      database.command("sql", "CREATE DOCUMENT TYPE Plain");
      database.command("sql", "CREATE PROPERTY Plain.s STRING");
      database.transaction(() -> {
        for (final String v : VALUES)
          database.newDocument("Plain").set("s", v).save();
      });

      for (final boolean asc : new boolean[] { true, false }) {
        final List<String> expected = ordered("Plain", asc, 0);
        assertThat(ordered("Ci", asc, 0)).isEqualTo(expected);
        assertThat(ordered("Nx", asc, 0)).isEqualTo(expected);
        assertThat(ordered("Ci", asc, 3)).isEqualTo(expected.subList(0, 3));
      }
      assertThat(ordered("Ci", true, 3)).containsExactly("AY", "AZ", "AZb");
    }

    @Test
    void limitedOrderByOnACompositeFoldedIndexMatchesTheUnindexedAnswer() {
      database.command("sql", "CREATE DOCUMENT TYPE Comp");
      database.command("sql", "CREATE PROPERTY Comp.a INTEGER");
      database.command("sql", "CREATE PROPERTY Comp.s STRING");
      database.command("sql", "CREATE INDEX ON Comp (a, s COLLATE ci) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE CompPlain");
      database.command("sql", "CREATE PROPERTY CompPlain.a INTEGER");
      database.command("sql", "CREATE PROPERTY CompPlain.s STRING");
      database.transaction(() -> {
        for (final String v : VALUES) {
          database.newDocument("Comp").set("a", 1).set("s", v).save();
          database.newDocument("CompPlain").set("a", 1).set("s", v).save();
        }
      });

      for (final boolean asc : new boolean[] { true, false }) {
        final List<String> expected = database.select().fromType("CompPlain").where().property("a").eq().value(1)
            .orderBy("s", asc).limit(4).documents().toList().stream().map(d -> d.getString("s")).toList();
        final List<String> actual = database.select().fromType("Comp").where().property("a").eq().value(1)
            .orderBy("s", asc).limit(4).documents().toList().stream().map(d -> d.getString("s")).toList();
        assertThat(actual).isEqualTo(expected);
      }
    }

    @Test
    void limitedEqualityOnAFoldedIndexStillAnswersTheExactValue() {
      database.command("sql", "CREATE DOCUMENT TYPE Eq");
      database.command("sql", "CREATE PROPERTY Eq.s STRING");
      database.command("sql", "CREATE PROPERTY Eq.n INTEGER");
      database.command("sql", "CREATE INDEX ON Eq (s COLLATE ci) NOTUNIQUE");
      database.transaction(() -> {
        // the four values fold to one key; an equality predicate still matches the exact value only (unchanged by #8937)
        final String[] same = { "abc", "ABC", "Abc", "aBc" };
        for (int i = 0; i < same.length; i++)
          database.newDocument("Eq").set("s", same[i]).set("n", i).save();
      });
      final List<String> asc = database.select().fromType("Eq").where().property("s").eq().value("abc")
          .orderBy("s", true).limit(2).documents().toList().stream().map(d -> d.getString("s")).toList();
      assertThat(asc).containsExactly("abc");
      final List<String> desc = database.select().fromType("Eq").where().property("s").eq().value("abc")
          .orderBy("s", false).limit(2).documents().toList().stream().map(d -> d.getString("s")).toList();
      assertThat(desc).containsExactly("abc");
    }
  }

  /**
   * Regression for #9302: a range cursor over a COLLATE ci index yielded a different set of rows.
   * Regression test for issue #9302: the native {@code select()} range cursor (gt/ge/lt/le/between) was built over a
   * {@code COLLATE ci} index, whose keys are folded, so the cursor yielded a different set of rows than the unindexed query and
   * the residual filter could not bring back the rows it never returned.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RangeFoldedIndex extends TestHelper {

    private static final String[] VALUES = { "AZb", "AZ", "azc", "Az", "AY", "B", "a[", "abc", "Mzz", "m", "Zoo", "apple", "Banana" };

    private void load(final String type, final String index) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
      if (index != null)
        database.command("sql", index);
      database.transaction(() -> {
        for (final String v : VALUES)
          database.newDocument(type).set("s", v).save();
      });
    }

    private static List<String> sorted(final SelectWhereAfterBlock query) {
      return query.documents().toList().stream().map((final Document d) -> d.getString("s")).sorted().toList();
    }

    private SelectWhereLeftBlock where(final String type) {
      return database.select().fromType(type).where();
    }

    private List<String> run(final String type, final String op) {
      final SelectWhereOperatorBlock property = where(type).property("s");
      return sorted(switch (op) {
        case "lt" -> property.lt().value("m");
        case "le" -> property.le().value("m");
        case "gt" -> property.gt().value("B");
        case "ge" -> property.ge().value("B");
        default -> property.between().values("B", "m");
      });
    }

    @Test
    void everyRangeOperatorOnAFoldedIndexMatchesTheUnindexedAnswer() {
      load("Ci", "CREATE INDEX ON Ci (s COLLATE ci) NOTUNIQUE");
      load("Cs", "CREATE INDEX ON Cs (s) NOTUNIQUE");
      load("Plain", null);

      for (final String op : new String[] { "lt", "le", "gt", "ge", "between" }) {
        final List<String> expected = run("Plain", op);
        assertThat(expected).as(op).isNotEmpty();
        assertThat(run("Ci", op)).as("ci " + op).isEqualTo(expected);
        assertThat(run("Cs", op)).as("cs " + op).isEqualTo(expected);
      }
    }

    @Test
    void rangeUnderOrOnAFoldedIndexStaysComplete() {
      load("Ci", "CREATE INDEX ON Ci (s COLLATE ci) NOTUNIQUE");
      load("Plain", null);

      final List<String> expected = sorted(where("Plain").property("s").gt().value("B").or().property("s").eq().value("AY"));
      assertThat(expected).contains("AY", "abc");
      assertThat(sorted(where("Ci").property("s").gt().value("B").or().property("s").eq().value("AY"))).isEqualTo(expected);
    }
  }
}
