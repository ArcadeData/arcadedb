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
    // THE NESTED CURSORS UNDER AN 'OR' ARE MERGED BY MultiIndexCursor - THE SAME UNION SHAPE AS THE BOOLEAN 'OR' - SO
    // THIS TREE IS EXACTLY INDEXED TOO, AND THE CAP MUST STILL ACCOUNT FOR skip
    final long count = database.select().fromType("T").where()//
        .property("n").lt().value(10)//
        .or().property("n").ge().value(990)//
        .skip(5).limit(10).count();
    assertThat(count).isEqualTo(10);
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
  void inLeafUnderOrWithSkipAndLimitPagesCorrectly() {
    // CODE REVIEW ON #6571: COMBINES THE in_op NESTED-CURSOR SHARING (inOperatorWithSkipAndLimitPagesCorrectly) WITH
    // THE or-MERGE SHAPE (orOfTwoIndexedRangesWithSkipAndLimitPagesCorrectly) TO MAKE EXPLICIT, RATHER THAN JUST
    // INFERRED, THAT SHARING THE WHOLE TREE'S indexCandidateLimit WITH A NESTED PER-VALUE CURSOR STAYS SAFE EVEN WHEN
    // THAT CURSOR IS ALSO ONE CHILD OF AN OUTER or-MERGED MultiIndexCursor.
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
    // CODE REVIEW ON #6571: Select.compile() WRAPS A LONE CONDITION (NO and()/or() CALLED) IN A SYNTHETIC 'run' NODE
    // (Select.setLogic()'S "1ST TIME ONLY" BRANCH), SO THE ROOT'S left IS A SelectTreeNode EVEN THOUGH THE WHOLE TREE
    // IS A SINGLE LEAF. A RESULT-COUNT ASSERTION CANNOT CATCH A REGRESSION HERE: THE LAZY-PULL CONSUMERS ALREADY STOP
    // AT skip + limit EVEN WHEN THE CANDIDATE CAP ITSELF STAYS AT -1, SO THIS CHECKS THE COMPUTED CAP DIRECTLY.
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
  void andUnderOrIsNeverTreatedAsExactlyIndexed() {
    // (a = 'x' AND b = 'set') OR n = 5: PRECEDENCE-DRIVEN Select.setLogic() BUILDS A NESTED and NODE UNDER THE or
    // ROOT (and HAS HIGHER PRECEDENCE THAN or). isWhereExactlyIndexed() MUST STAY CONSERVATIVE FOR THE NESTED and
    // REGARDLESS OF THE or WRAPPER AROUND IT, SINCE filterWithIndexesFinalNode() STILL ONLY KEEPS ONE OF THE and'S
    // TWO CHILD CURSORS.
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
    // isWhereExactlyIndexed() MUST STILL TREAT IT CONSERVATIVELY (filterWithIndexesFinalNode() NEVER BUILDS A
    // CURSOR FOR IT), SO THE TREE IS BUILT BY HAND HERE
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
    // CODE REVIEW ON #6571, ROUND 3: SelectIterator.fetchResultInCaseOfOrderBy() MATERIALIZES BY DRAINING THE FULL
    // ITERATOR WHENEVER THE REQUESTED ORDER DOESN'T TRIVIALLY MATCH THE (ALWAYS ASCENDING) INDEX SCAN
    // filterWithIndexesFinalNode() PERFORMS - BUT THE CANDIDATE CAP THIS PR RESTORES WOULD STOP THAT DRAIN AT
    // skip + limit, LETTING THE IN-MEMORY SORT SEE ONLY THE FIRST FEW ASCENDING CANDIDATES INSTEAD OF EVERY MATCH.
    // A DESCENDING orderBy() ON THE SAME PROPERTY THE WHERE CLAUSE INDEXES MUST DISABLE THE CAP TOO.
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
