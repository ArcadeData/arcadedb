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
  void compositeIndexPrefixMatchWithParameterBinding() {
    // matchCompositeIndex() HAS A DEDICATED SelectParameterValue BRANCH FOR THE MATCHED PREFIX'S KEYS, MIRRORING THE
    // PRE-EXISTING SINGLE-PROPERTY PATH - EXERCISE IT WITH .parameter(...) RATHER THAN .value(...), THE WAY
    // SelectIndexExecutionTest DOES FOR THAT PATH
    final SelectCompiled select = database.select().fromType("Supplier")//
        .where().property("key1").eq().parameter("k1")//
        .and().property("key2").eq().parameter("k2").compile();

    final List<Vertex> list = select.parameter("k1", "a").parameter("k2", "b").vertices().toList();

    assertThat(list).hasSize(GROUP_SIZE);
    list.forEach(v -> {
      assertThat(v.getString("key1")).isEqualTo("a");
      assertThat(v.getString("key2")).isEqualTo("b");
    });
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
  void compositeIndexTieBreakFallsBackToAlphabeticalOrder() {
    // TWO COMPOSITE INDEXES TIE ON BOTH MATCHED PREFIX LENGTH (1, JUST groupKey) AND UNIQUENESS (BOTH NON-UNIQUE):
    // THE TIE FALLS THROUGH TO THE candidates SORT-BY-NAME ORDERING. THE AUTO-GENERATED NAME IS
    // "<Type>[<properties>]" (SEE TypeIndex.updateTypeName()), SO "AlphaTie[groupKey,valA]" SORTS BEFORE
    // "AlphaTie[groupKey,valB]" - OBSERVED HERE BY WHETHER ORDER BY valA GETS ELIDED (IT MUST, SINCE THE
    // ALPHABETICALLY-FIRST INDEX'S TRAILING PROPERTY IS valA, NOT valB)
    final VertexType alphaTie = database.getSchema().createVertexType("AlphaTie");
    alphaTie.createProperty("groupKey", Type.STRING);
    alphaTie.createProperty("valA", Type.LONG);
    alphaTie.createProperty("valB", Type.LONG);
    alphaTie.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "groupKey", "valA");
    alphaTie.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "groupKey", "valB");

    database.transaction(() -> {
      for (int i = 0; i < GROUP_SIZE; i++)
        database.newVertex("AlphaTie").set("groupKey", "g", "valA", (long) i, "valB", (long) i).save();
    });

    final SelectCompiled select = database.select().fromType("AlphaTie")//
        .where().property("groupKey").eq().value("g")//
        .orderBy("valA", false)//
        .limit(1)//
        .compile();

    final SelectIterator<Vertex> result = select.vertices();
    final Vertex first = result.nextOrNull();

    assertThat(first).isNotNull();
    assertThat(first.getLong("valA")).isEqualTo(GROUP_SIZE - 1);
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    // ORDER BY valA MUST BE ELIDED (NOT A FULL MATERIALIZE-AND-SORT): ONLY THE SINGLE RETURNED CANDIDATE IS EVALUATED
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(1L);
  }

  @Test
  void compositeIndexMultiColumnOrderByDisablesElision() {
    // A MULTI-COLUMN ORDER BY (size() > 1) MUST NEVER BE TREATED AS ELIDED, EVEN THOUGH ITS FIRST COLUMN MATCHES THE
    // COMPOSITE INDEX'S TRAILING PROPERTY - orderByElided EXPLICITLY REQUIRES select.orderBy.size() == 1. ROWS
    // SHARING THE SAME PRIMARY (orderedAt) VALUE MUST STILL COME OUT ORDERED BY THE SECONDARY COLUMN (tiebreaker),
    // WHICH ONLY A FULL MATERIALIZE-AND-SORT (NOT A BARE INDEX SCAN OVER orderedAt ALONE) CAN GUARANTEE
    final VertexType multiSort = database.getSchema().createVertexType("MultiSort");
    multiSort.createProperty("groupKey", Type.STRING);
    multiSort.createProperty("orderedAt", Type.LONG);
    multiSort.createProperty("tiebreaker", Type.LONG);
    multiSort.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "groupKey", "orderedAt");

    database.transaction(() -> {
      // THREE ROWS SHARE orderedAt=100, DIFFERING ONLY BY tiebreaker - INSERTED OUT OF tiebreaker ORDER
      database.newVertex("MultiSort").set("groupKey", "g", "orderedAt", 100L, "tiebreaker", 3L).save();
      database.newVertex("MultiSort").set("groupKey", "g", "orderedAt", 100L, "tiebreaker", 1L).save();
      database.newVertex("MultiSort").set("groupKey", "g", "orderedAt", 100L, "tiebreaker", 2L).save();
      database.newVertex("MultiSort").set("groupKey", "g", "orderedAt", 200L, "tiebreaker", 0L).save();
    });

    final SelectCompiled select = database.select().fromType("MultiSort")//
        .where().property("groupKey").eq().value("g")//
        .orderBy("orderedAt", false)//
        .orderBy("tiebreaker", true)//
        .compile();

    final List<Vertex> list = select.vertices().toList();

    assertThat(list).hasSize(4);
    assertThat(list.get(0).getLong("orderedAt")).isEqualTo(200L);
    assertThat(list.get(1).getLong("tiebreaker")).isEqualTo(1L);
    assertThat(list.get(2).getLong("tiebreaker")).isEqualTo(2L);
    assertThat(list.get(3).getLong("tiebreaker")).isEqualTo(3L);
  }

  @Test
  void compositeIndexPartialPrefixDefersToStandaloneIndexOnUnmatchedProperty() {
    // A NON-UNIQUE COMPOSITE INDEX (key1, key2, key3) COEXISTS WITH A STANDALONE UNIQUE INDEX ON key3 ALONE - A REAL
    // SCHEMA SHAPE (ONE INDEX FOR A QUERY PATTERN, ANOTHER ENFORCING A UNIQUENESS CONSTRAINT). key2 IS UNBOUND, SO
    // THE COMPOSITE PREFIX MATCH STOPS AT key1 (prefixLength=1) AND WOULD OTHERWISE SCAN EVERY key1='a' ROW - key3,
    // BOUND BY EQUALITY BUT OUTSIDE THAT PREFIX, MUST INSTEAD DEFER TO ITS OWN STANDALONE UNIQUE INDEX, WHICH
    // ANSWERS THE QUERY WITH A SINGLE-ROW LOOKUP INSTEAD OF SCANNING THE WHOLE key1='a' GROUP
    final VertexType precedence = database.getSchema().createVertexType("PrecedenceCheck");
    precedence.createProperty("key1", Type.STRING);
    precedence.createProperty("key2", Type.STRING);
    precedence.createProperty("key3", Type.STRING);
    precedence.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "key1", "key2", "key3");
    precedence.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "key3");

    database.transaction(() -> {
      for (int i = 0; i < GROUP_SIZE; i++)
        database.newVertex("PrecedenceCheck").set("key1", "a", "key2", "k" + i, "key3", "u" + i).save();
      for (int i = 0; i < OTHER_SIZE; i++)
        database.newVertex("PrecedenceCheck").set("key1", "other", "key2", "k" + i, "key3", "other" + i).save();
    });

    final SelectCompiled select = database.select().fromType("PrecedenceCheck")//
        .where().property("key1").eq().value("a")//
        .and().property("key3").eq().value("u3")//
        .compile();

    final SelectIterator<Vertex> result = select.vertices();
    final List<Vertex> list = result.toList();

    assertThat(list).hasSize(1);
    assertThat(list.getFirst().getString("key3")).isEqualTo("u3");
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    // THE STANDALONE UNIQUE INDEX ON key3 MUST WIN OVER THE COMPOSITE PREFIX MATCH: ONLY THE SINGLE MATCHING RECORD
    // IS EVALUATED, NOT THE WHOLE key1='a' GROUP
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo(1L);
  }

  @Test
  void compositeIndexPartialPrefixKeepsMatchWhenUnmatchedStandaloneIndexIsNotUnique() {
    // UNLIKE compositeIndexPartialPrefixDefersToStandaloneIndexOnUnmatchedProperty, THE STANDALONE INDEX ON key3
    // HERE IS NON-UNIQUE: DEFERRING TO IT WOULD GIVE UP NO GUARANTEED PRECISION (evaluateWhere() STILL NARROWS THE
    // RESULT DOWN CORRECTLY EITHER WAY, AND A NON-UNIQUE INDEX COULD EVEN BE LESS SELECTIVE THAN THE COMPOSITE
    // PREFIX'S OWN COMBINED LEADING PROPERTIES) - SO THE COMPOSITE PREFIX MATCH ON key1 MUST STILL WIN RATHER THAN
    // FALLING BACK TO A SINGLE-PROPERTY PATH
    final VertexType precedence = database.getSchema().createVertexType("PrecedenceCheckNonUnique");
    precedence.createProperty("key1", Type.STRING);
    precedence.createProperty("key2", Type.STRING);
    precedence.createProperty("key3", Type.STRING);
    precedence.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "key1", "key2", "key3");
    precedence.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "key3");

    database.transaction(() -> {
      for (int i = 0; i < GROUP_SIZE; i++)
        database.newVertex("PrecedenceCheckNonUnique").set("key1", "a", "key2", "k" + i, "key3", "u" + i).save();
      for (int i = 0; i < OTHER_SIZE; i++)
        database.newVertex("PrecedenceCheckNonUnique").set("key1", "other", "key2", "k" + i, "key3", "other" + i).save();
    });

    final SelectCompiled select = database.select().fromType("PrecedenceCheckNonUnique")//
        .where().property("key1").eq().value("a")//
        .and().property("key3").eq().value("u3")//
        .compile();

    final SelectIterator<Vertex> result = select.vertices();
    final List<Vertex> list = result.toList();

    assertThat(list).hasSize(1);
    assertThat(list.getFirst().getString("key3")).isEqualTo("u3");
    assertThat(result.getMetrics().get("usedIndexes")).isEqualTo(1);
    // THE COMPOSITE PREFIX MATCH IS KEPT: EVERY key1='a' ROW IS EVALUATED (NOT JUST THE ONE MATCHING key3), SINCE A
    // NON-UNIQUE STANDALONE INDEX ON key3 DOES NOT TRIGGER DEFERRAL
    assertThat(result.getMetrics().get("evaluatedRecords")).isEqualTo((long) GROUP_SIZE);
  }

  @Test
  void compositeHashIndexPartialPrefixFallsBackToScan() {
    // A COMPOSITE HASH INDEX DOES NOT SUPPORT ORDERED ITERATIONS (TypeIndex.supportsOrderedIterations() == false),
    // SO A PARTIAL-PREFIX MATCH AGAINST IT MUST NOT ATTEMPT range() - WHICH WOULD THROW UnsupportedOperationException
    // - AND MUST INSTEAD FALL BACK TO THE PRE-EXISTING isTheNodeFullyIndexed()/filterWithIndexes() PATH (A FULL SCAN
    // HERE, SINCE NEITHER PROPERTY HAS ITS OWN STANDALONE INDEX), EXACTLY AS BEFORE THIS COMPOSITE-INDEX SUPPORT
    // EXISTED. A UNIQUE_HASH INDEX ON (@out, @in) IS A COMMON EDGE-UNIQUENESS IDIOM IN THIS CODEBASE
    // (Issue5677HashIndexLinkKeyTest) THAT MUST KEEP WORKING WHEN ONLY ONE ENDPOINT IS BOUND.
    final VertexType hashType = database.getSchema().createVertexType("HashComposite");
    hashType.createProperty("hashKey1", Type.STRING);
    hashType.createProperty("hashKey2", Type.STRING);
    hashType.createTypeIndex(Schema.INDEX_TYPE.HASH, false, "hashKey1", "hashKey2");

    database.transaction(() -> {
      for (int i = 0; i < GROUP_SIZE; i++)
        database.newVertex("HashComposite").set("hashKey1", "a", "hashKey2", "k" + i).save();
      for (int i = 0; i < OTHER_SIZE; i++)
        database.newVertex("HashComposite").set("hashKey1", "other", "hashKey2", "k" + i).save();
    });

    // ONLY hashKey1 IS BOUND: A PARTIAL PREFIX OF THE 2-PROPERTY HASH INDEX
    final SelectCompiled select = database.select().fromType("HashComposite")//
        .where().property("hashKey1").eq().value("a").compile();

    final List<Vertex> list = select.vertices().toList();

    assertThat(list).hasSize(GROUP_SIZE);
    list.forEach(v -> assertThat(v.getString("hashKey1")).isEqualTo("a"));
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
