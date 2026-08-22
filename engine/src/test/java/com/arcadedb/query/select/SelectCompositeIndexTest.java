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
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/6592: a composite (multi-property) index was
 * never considered by the native {@link Select} query builder unless every one of its properties appeared as a
 * separate equality leaf in the where-tree - a plain AND of only the index's LEADING properties fell back to a full
 * type scan even though the index could answer it with a partial-key lookup.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectCompositeIndexTest extends TestHelper {

  private static final int GROUP_SIZE = 5;
  private static final int OTHER_SIZE = 495;

  public SelectCompositeIndexTest() {
    autoStartTx = true;
  }

  @Override
  protected void beginTest() {
    final VertexType supplier = database.getSchema().createVertexType("Supplier");
    supplier.createProperty("key1", Type.STRING);
    supplier.createProperty("key2", Type.STRING);
    supplier.createProperty("orderedAt", Type.LONG);
    supplier.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "key1", "key2", "orderedAt");

    database.transaction(() -> {
      for (int i = 0; i < GROUP_SIZE; i++)
        database.newVertex("Supplier").set("key1", "a", "key2", "b", "orderedAt", (long) i).save();
      for (int i = 0; i < OTHER_SIZE; i++)
        database.newVertex("Supplier").set("key1", "other", "key2", "other", "orderedAt", (long) i).save();
    });
  }

  @Test
  void compositeIndexPrefixMatchIsUsed() {
    final SelectCompiled select = database.select().fromType("Supplier")//
        .where().property("key1").eq().value("a")//
        .and().property("key2").eq().value("b").compile();

    final SelectIterator<Vertex> result = select.vertices();
    final List<Vertex> list = result.toList();

    assertThat(list).hasSize(GROUP_SIZE);
    list.forEach(v -> {
      assertThat(v.getString("key1")).isEqualTo("a");
      assertThat(v.getString("key2")).isEqualTo("b");
    });

    // THE COMPOSITE INDEX MUST BE USED (NOT A FULL TYPE SCAN) EVEN THOUGH ONLY A LEADING PREFIX (key1, key2) OF THE
    // 3-PROPERTY INDEX (key1, key2, orderedAt) IS BOUND BY EQUALITY
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo((long) GROUP_SIZE);
  }

  @Test
  void compositeIndexPrefixMatchWithOrderByDescLimitOne() {
    // EXACT SCENARIO FROM ISSUE #6592: EQUALITY ON THE LEADING TWO PROPERTIES OF THE COMPOSITE INDEX, ORDER BY THE
    // TRAILING PROPERTY DESCENDING, LIMIT 1 - MUST RETURN THE MOST RECENT MATCH WITHOUT A FULL TYPE SCAN NOR SORT
    final SelectCompiled select = database.select().fromType("Supplier")//
        .where().property("key1").eq().value("a")//
        .and().property("key2").eq().value("b")//
        .orderBy("orderedAt", false)//
        .limit(1)//
        .compile();

    final SelectIterator<Vertex> result = select.vertices();
    final Vertex first = result.nextOrNull();

    assertThat(first).isNotNull();
    assertThat(first.getString("key1")).isEqualTo("a");
    assertThat(first.getString("key2")).isEqualTo("b");
    assertThat(first.getLong("orderedAt")).isEqualTo(GROUP_SIZE - 1);
    assertThat(result.hasNext()).isFalse();

    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    // THE ORDER BY IS SATISFIED BY THE INDEX'S OWN SCAN DIRECTION: ONLY THE (SINGLE) RETURNED CANDIDATE IS EVALUATED,
    // NOT THE WHOLE key1='a' AND key2='b' GROUP NOR THE FULL TYPE
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(1L);
  }

  @Test
  void compositeIndexPartialPrefixStillUsed() {
    // ONLY key1 IS BOUND: STILL A VALID (SHORTER) PREFIX OF THE SAME COMPOSITE INDEX
    final SelectCompiled select = database.select().fromType("Supplier")//
        .where().property("key1").eq().value("a").compile();

    final SelectIterator<Vertex> result = select.vertices();
    final List<Vertex> list = result.toList();

    assertThat(list).hasSize(GROUP_SIZE);
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo((long) GROUP_SIZE);
  }

  @Test
  void compositeIndexFullKeyMatchIsUsed() {
    // ALL THREE PROPERTIES OF THE COMPOSITE INDEX ARE BOUND BY EQUALITY: THE KEY'S ARITY MATCHES THE INDEX'S OWN
    // EXACTLY, SO THIS GOES THROUGH THE PLAIN get() LOOKUP RATHER THAN THE PARTIAL-KEY range() SCAN
    final SelectCompiled select = database.select().fromType("Supplier")//
        .where().property("key1").eq().value("a")//
        .and().property("key2").eq().value("b")//
        .and().property("orderedAt").eq().value(3L).compile();

    final SelectIterator<Vertex> result = select.vertices();
    final List<Vertex> list = result.toList();

    assertThat(list).hasSize(1);
    assertThat(list.getFirst().getLong("orderedAt")).isEqualTo(3L);
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(1L);
  }

  @Test
  void compositeIndexPrefixMatchWithSkipAndLimit() {
    // THE #6565 CANDIDATE CAP (skip + limit) ON THE COMPOSITE-INDEX/ORDER-BY-ELIDED PATH MUST BEHAVE LIKE IT DOES ON
    // THE SINGLE-PROPERTY PATH (SEE Issue6565SelectIndexCandidateLimitTest)
    final SelectCompiled select = database.select().fromType("Supplier")//
        .where().property("key1").eq().value("a")//
        .and().property("key2").eq().value("b")//
        .orderBy("orderedAt", true)//
        .skip(1)//
        .limit(2)//
        .compile();

    final List<Vertex> list = select.vertices().toList();

    assertThat(list).hasSize(2);
    assertThat(list.get(0).getLong("orderedAt")).isEqualTo(1L);
    assertThat(list.get(1).getLong("orderedAt")).isEqualTo(2L);
  }

  @Test
  void compositeIndexPrefixWithTrailingRangeAndLimitStaysCorrect() {
    // A THIRD LEAF ON THE COMPOSITE INDEX'S OWN TRAILING PROPERTY, BUT VIA gt (NOT eq), IS NOT PART OF THE MATCHED
    // PREFIX (ONLY eq LEAVES ARE COLLECTED INTO andEqLeaves) - exactMatch MUST STAY false SO THE #6565 CANDIDATE CAP
    // STAYS DISABLED. WERE IT WRONGLY CAPPED AT skip + limit, THE UNDERLYING ASCENDING RANGE SCAN WOULD HAND OUT
    // ONLY THE FIRST 2 (LOWEST orderedAt) CANDIDATES - orderedAt 0 AND 1 - BOTH OF WHICH FAIL THE gt(2) FILTER,
    // YIELDING ZERO RESULTS INSTEAD OF THE CORRECT TWO (orderedAt 3 AND 4)
    final SelectCompiled select = database.select().fromType("Supplier")//
        .where().property("key1").eq().value("a")//
        .and().property("key2").eq().value("b")//
        .and().property("orderedAt").gt().value(2L)//
        .limit(2)//
        .compile();

    final List<Vertex> list = select.vertices().toList();

    assertThat(list).hasSize(2);
    list.forEach(v -> assertThat(v.getLong("orderedAt")).isGreaterThan(2L));
  }

  @Test
  void compositeIndexTieBreakPrefersUniqueIndex() {
    // TWO COMPOSITE INDEXES TIE ON MATCHED PREFIX LENGTH (2): (tieKey1, tieKey2, payload) NON-UNIQUE AND
    // (tieKey1, tieKey3) UNIQUE. THE TIE MUST BE BROKEN TOWARD THE UNIQUE ONE - OBSERVED INDIRECTLY HERE SINCE A
    // UNIQUE get() LOOKUP EVALUATES EXACTLY 1 CANDIDATE, WHILE THE NON-UNIQUE INDEX'S (tieKey1, tieKey2) PREFIX WOULD
    // HAVE MATCHED EVERY ROW IN THE GROUP BEFORE evaluateWhere() COULD FILTER DOWN BY tieKey3
    final VertexType tieBreak = database.getSchema().createVertexType("TieBreak");
    tieBreak.createProperty("tieKey1", Type.STRING);
    tieBreak.createProperty("tieKey2", Type.STRING);
    tieBreak.createProperty("tieKey3", Type.STRING);
    tieBreak.createProperty("payload", Type.LONG);
    tieBreak.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "tieKey1", "tieKey2", "payload");
    tieBreak.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "tieKey1", "tieKey3");

    database.transaction(() -> {
      for (int i = 0; i < GROUP_SIZE; i++)
        database.newVertex("TieBreak").set("tieKey1", "x", "tieKey2", "y", "tieKey3", "k" + i, "payload", (long) i).save();
    });

    final SelectCompiled select = database.select().fromType("TieBreak")//
        .where().property("tieKey1").eq().value("x")//
        .and().property("tieKey2").eq().value("y")//
        .and().property("tieKey3").eq().value("k3")//
        .compile();

    final SelectIterator<Vertex> result = select.vertices();
    final List<Vertex> list = result.toList();

    assertThat(list).hasSize(1);
    assertThat(list.getFirst().getLong("payload")).isEqualTo(3L);
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(1L);
  }

  @Test
  void compositeIndexNotUsedUnderOr() {
    // AN 'or' MUST NOT ATTEMPT THE COMPOSITE PREFIX MATCH: NEITHER PROPERTY HAS ITS OWN STANDALONE INDEX HERE, SO NO
    // INDEX CAN BE USED AND THE QUERY FALLS BACK TO A FULL TYPE SCAN, JUST LIKE BEFORE THE COMPOSITE-INDEX SUPPORT
    final SelectCompiled select = database.select().fromType("Supplier")//
        .where().property("key1").eq().value("a")//
        .or().property("key2").eq().value("b").compile();

    final SelectIterator<Vertex> result = select.vertices();
    result.toList();

    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(0);
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo((long) (GROUP_SIZE + OTHER_SIZE));
  }
}
