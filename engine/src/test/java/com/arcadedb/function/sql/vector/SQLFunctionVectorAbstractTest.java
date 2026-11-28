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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.utility.IntHashSet;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Direct tests of the helpers {@link SQLFunctionVectorAbstract} shares between {@code vector.neighbors},
 * {@code vector.sparseNeighbors}, {@code vector.fuse}, {@code vector.mmr}, {@code vector.rerank} and the other vector
 * functions. They are reached through SQL only by way of those functions, so a change here that a function-level test happens not
 * to notice (a score read with the wrong sign, a row dropped for its RID, a filter that silently accepts a malformed element)
 * would change what every one of them answers.
 */
class SQLFunctionVectorAbstractTest extends TestHelper {
  private final SQLFunctionVectorMagnitude function = new SQLFunctionVectorMagnitude();

  @Test
  void aliasIsTheCamelCaseOfTheDottedName() {
    assertThat(function.getName()).isEqualTo("vector.magnitude");
    assertThat(function.getAlias()).isEqualTo("vectorMagnitude");
  }

  @Test
  void int8CodeAcceptsExactlyTheByteRange() {
    assertThat(SQLFunctionVectorAbstract.toInt8Code(-128)).isEqualTo((byte) -128);
    assertThat(SQLFunctionVectorAbstract.toInt8Code(127L)).isEqualTo((byte) 127);
    assertThat(SQLFunctionVectorAbstract.toInt8Code(0)).isEqualTo((byte) 0);
    // a whole number stored as a decimal is still a whole number
    assertThat(SQLFunctionVectorAbstract.toInt8Code(5.0d)).isEqualTo((byte) 5);
    assertThat(SQLFunctionVectorAbstract.toInt8Code(new BigDecimal("-7"))).isEqualTo((byte) -7);

    for (final Object bad : new Object[] { 128, -129, 200L, 2.5d, Double.NaN, Double.POSITIVE_INFINITY, 1e30d,
        new BigDecimal("1.00000000000000000001"), "5", null })
      assertThatThrownBy(() -> SQLFunctionVectorAbstract.toInt8Code(bad))
          .as("int8 code from %s", bad)
          .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void integerArgumentIsWholeAndNamedInTheError() {
    assertThat(function.parseInteger(7, "k")).isEqualTo(7);
    assertThat(function.parseInteger(7L, "k")).isEqualTo(7);
    assertThat(function.parseInteger(7.0d, "k")).isEqualTo(7);
    assertThat(function.parseInteger("  42 ", "k")).isEqualTo(42);
    assertThat(function.parseInteger(Integer.MIN_VALUE, "k")).isEqualTo(Integer.MIN_VALUE);

    for (final Object bad : new Object[] { 2.5d, 3_000_000_000L, "abc", "", "1.5", Double.NaN })
      assertThatThrownBy(() -> function.parseInteger(bad, "beam"))
          .as("integer from %s", bad)
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("vector.magnitude beam must be an integer");

    assertThatThrownBy(() -> function.parseInteger(null, "beam"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("vector.magnitude beam");
  }

  @Test
  void neighborCountAllowsZeroAndRefusesNegative() {
    assertThat(function.parseNeighborCount(0)).isZero();
    assertThat(function.parseNeighborCount("10")).isEqualTo(10);
    assertThatThrownBy(() -> function.parseNeighborCount(-1))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("k must not be negative, got -1");
    assertThatThrownBy(() -> function.parseNeighborCount(1.5d))
        .isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> function.parseNeighborCount(null))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void vectorByIdLookupQuotesEveryNameAndBindsTheKey() {
    // every name goes through Identifier.quote, plain ones included, and the key is a bound parameter
    assertThat(SQLFunctionVectorAbstract.buildVectorByIdLookup("Person", "embedding", "id"))
        .isEqualTo("SELECT `embedding` FROM `Person` WHERE `id` = ? LIMIT 1");
    assertThat(SQLFunctionVectorAbstract.buildVectorByIdLookup("Person Node", "my vec", "the id"))
        .isEqualTo("SELECT `my vec` FROM `Person Node` WHERE `the id` = ? LIMIT 1");
  }

  @Test
  void nestedFieldReadsFlatMapAndEmbeddedPaths() {
    database.getSchema().createDocumentType("NestedHolder");
    database.getSchema().createDocumentType("NestedEmb");
    final MutableDocument[] holder = new MutableDocument[1];
    database.transaction(() -> {
      holder[0] = database.newDocument("NestedHolder");
      holder[0].set("flat", "f");
      final Map<String, Object> inner = new HashMap<>();
      inner.put("id", 7);
      inner.put("nothing", null);
      final Map<String, Object> meta = new HashMap<>();
      meta.put("author", "ada");
      meta.put("source", inner);
      holder[0].set("metadata", meta);
      holder[0].newEmbeddedDocument("NestedEmb", "emb").set("deep", "d");
      holder[0].save();
    });

    final Document record = holder[0];
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "flat")).isEqualTo("f");
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "metadata.author")).isEqualTo("ada");
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "metadata.source.id")).isEqualTo(7);
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "emb.deep")).isEqualTo("d");

    // a missing segment, or a primitive in the middle of the path, answers null rather than throwing
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "missing")).isNull();
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "missing.author")).isNull();
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "metadata.missing.id")).isNull();
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "metadata.source.nothing.deeper")).isNull();
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "flat.length")).isNull();
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "metadata.author.length")).isNull();

    assertThat(SQLFunctionVectorAbstract.readNestedField(null, "flat")).isNull();
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, null)).isNull();
    assertThat(SQLFunctionVectorAbstract.readNestedField(record, "")).isNull();
  }

  @Test
  void ridOfARowComesFromTheShapesTheVectorPipelinesEmit() {
    final RID rid = new RID(3, 4);
    final RID other = new RID(3, 5);

    final Map<String, Object> atRid = new HashMap<>();
    atRid.put("@rid", rid);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(atRid)).isEqualTo(rid);

    final Map<String, Object> plainRid = new HashMap<>();
    plainRid.put("rid", rid);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(plainRid)).isEqualTo(rid);

    // @rid wins over rid
    final Map<String, Object> both = new HashMap<>();
    both.put("@rid", rid);
    both.put("rid", other);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(both)).isEqualTo(rid);

    // an @rid that is not usable does not fall back to rid, and a non-RID value is no RID
    final Map<String, Object> text = new HashMap<>();
    text.put("@rid", "#3:4");
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(text)).isNull();
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(new HashMap<String, Object>())).isNull();

    database.getSchema().createDocumentType("RidRow");
    final MutableDocument[] doc = new MutableDocument[1];
    database.transaction(() -> {
      doc[0] = database.newDocument("RidRow").set("a", 1);
      doc[0].save();
    });
    final RID docRid = doc[0].getIdentity();

    // an identifiable value inside a map, and a bare identifiable row
    final Map<String, Object> identifiable = new HashMap<>();
    identifiable.put("rid", doc[0]);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(identifiable)).isEqualTo(docRid);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(doc[0])).isEqualTo(docRid);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(docRid)).isEqualTo(docRid);

    // a result: its identity first, then the @rid / rid properties
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(new ResultInternal(doc[0]))).isEqualTo(docRid);
    final ResultInternal projected = new ResultInternal();
    projected.setProperty("rid", other);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(projected)).isEqualTo(other);
    projected.setProperty("@rid", rid);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(projected)).isEqualTo(rid);
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(new ResultInternal())).isNull();

    assertThat(SQLFunctionVectorAbstract.extractRidFromRow(null)).isNull();
    assertThat(SQLFunctionVectorAbstract.extractRidFromRow("#3:4")).isNull();
  }

  @Test
  void scoreOfARowIsASimilarityAndADistanceIsNegated() {
    final Map<String, Object> row = new LinkedHashMap<>();
    row.put("distance", 0.25d);
    // vector.neighbors emits a distance: lower is better, so it is negated to read as "higher is better"
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(row)).isEqualTo(-0.25f);

    row.put("$score", 3);
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(row)).isEqualTo(3f);
    row.put("score", 0.5f);
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(row)).isEqualTo(0.5f);

    // a score that is not a number does not hide the next field
    row.put("score", "high");
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(row)).isEqualTo(3f);
    row.put("$score", "high");
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(row)).isEqualTo(-0.25f);
    row.put("distance", "far");
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(row)).isNaN();
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(new HashMap<String, Object>())).isNaN();

    final ResultInternal result = new ResultInternal();
    result.setProperty("distance", 2L);
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(result)).isEqualTo(-2f);
    result.setProperty("$score", 1.5d);
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(result)).isEqualTo(1.5f);
    result.setProperty("score", 9);
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(result)).isEqualTo(9f);
    result.setProperty("score", "high");
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(result)).isEqualTo(1.5f);
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(new ResultInternal())).isNaN();

    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow(null)).isNaN();
    assertThat(SQLFunctionVectorAbstract.extractScoreFromRow("score")).isNaN();
  }

  @Test
  void ridFilterReadsEveryDocumentedShapeAndRefusesTheRest() {
    database.getSchema().createDocumentType("FilterDoc");
    final MutableDocument[] doc = new MutableDocument[2];
    database.transaction(() -> {
      doc[0] = database.newDocument("FilterDoc").set("a", 1);
      doc[0].save();
      doc[1] = database.newDocument("FilterDoc").set("a", 2);
      doc[1].save();
    });
    final RID first = doc[0].getIdentity();
    final RID second = doc[1].getIdentity();
    final CommandContext context = new BasicCommandContext().setDatabase(database);

    // no option means no filter; an option that resolves to nothing is an EMPTY set, not null (#8959)
    assertThat(SQLFunctionVectorAbstract.parseRidFilter(null, "vector.neighbors", context)).isNull();
    assertThat(SQLFunctionVectorAbstract.parseRidFilter(List.of(), "vector.neighbors", context)).isNotNull().isEmpty();
    final List<Object> onlyNulls = new ArrayList<>();
    onlyNulls.add(null);
    onlyNulls.add(new ArrayList<>(onlyNulls));
    assertThat(SQLFunctionVectorAbstract.parseRidFilter(onlyNulls, "vector.neighbors", context)).isNotNull().isEmpty();

    final List<Object> mixed = new ArrayList<>();
    mixed.add(first);
    mixed.add(doc[1]);
    mixed.add(first.toString());
    mixed.add(null);
    final Set<RID> flat = SQLFunctionVectorAbstract.parseRidFilter(mixed, "vector.neighbors", context);
    assertThat(flat).containsExactlyInAnyOrder(first, second);

    // one level of nesting is flattened, nulls inside it skipped
    final List<Object> inner = new ArrayList<>();
    inner.add(second);
    inner.add(null);
    final List<Object> nested = new ArrayList<>();
    nested.add(inner);
    nested.add(first);
    assertThat(SQLFunctionVectorAbstract.parseRidFilter(nested, "vector.neighbors", context))
        .containsExactlyInAnyOrder(first, second);

    // a subquery row: the record itself, or exactly one RID-valued column
    final ResultInternal element = new ResultInternal(doc[0]);
    final ResultInternal oneColumn = new ResultInternal();
    oneColumn.setProperty("r", second);
    final ResultInternal oneTextColumn = new ResultInternal();
    oneTextColumn.setProperty("r", first.toString());
    assertThat(SQLFunctionVectorAbstract.parseRidFilter(List.of(element, oneColumn), "vector.neighbors", context))
        .containsExactlyInAnyOrder(first, second);
    assertThat(SQLFunctionVectorAbstract.parseRidFilter(List.of(oneTextColumn), "vector.neighbors", context))
        .containsExactly(first);

    // a typo in the projection stays loud rather than filtering nothing
    final ResultInternal twoColumns = new ResultInternal();
    twoColumns.setProperty("a", first);
    twoColumns.setProperty("b", second);
    assertThatThrownBy(() -> SQLFunctionVectorAbstract.parseRidFilter(List.of(twoColumns), "vector.neighbors", context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("vector.neighbors")
        .hasMessageContaining("project the RID alone");
    final ResultInternal numberColumn = new ResultInternal();
    numberColumn.setProperty("n", 5);
    assertThatThrownBy(() -> SQLFunctionVectorAbstract.parseRidFilter(List.of(numberColumn), "vector.neighbors", context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("project the RID alone");
    assertThatThrownBy(() -> SQLFunctionVectorAbstract.parseRidFilter(List.of("not a rid"), "vector.neighbors", context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("must contain RIDs, got: 'not a rid'");
    assertThatThrownBy(() -> SQLFunctionVectorAbstract.parseRidFilter(List.of(12), "vector.neighbors", context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("must contain RIDs, got: Integer");
    // two levels of nesting are refused by the element reader
    assertThatThrownBy(() -> SQLFunctionVectorAbstract.parseRidFilter(List.of(List.of(List.of(first))), "vector.neighbors", context))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void groupedFetchBudgetIsFiveCandidatesPerSlotAndAtLeastOnePerGroup() {
    assertThat(SQLFunctionVectorAbstract.groupedFetchBudget(3, 4)).isEqualTo(60L);
    assertThat(SQLFunctionVectorAbstract.groupedFetchBudget(10, 0)).isEqualTo(10L);
    assertThat(SQLFunctionVectorAbstract.groupedFetchBudget(10, 1)).isEqualTo(50L);
    assertThat(SQLFunctionVectorAbstract.groupedFetchBudget(0, 5)).isZero();
    // computed in long: a request near the int limits must not wrap into a small budget that waves it through the memory guard
    assertThat(SQLFunctionVectorAbstract.groupedFetchBudget(Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo((long) Integer.MAX_VALUE * Integer.MAX_VALUE * 5L);
  }

  @Test
  void staleTopUpRunsOnceWhenTheAnswerIsFullOrNothingWentStale() {
    final List<int[]> calls = new ArrayList<>();
    final ArrayList<Object> full = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(2, 3, 1_000, (g, s, stale) -> {
      calls.add(new int[] { g, s });
      return rows(6);
    });
    assertThat(full).hasSize(6);
    assertThat(calls).hasSize(1);
    assertThat(calls.getFirst()).containsExactly(2, 3);

    // a short answer with no stale candidate is the corpus's whole answer
    calls.clear();
    final ArrayList<Object> short1 = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(2, 3, 1_000, (g, s, stale) -> {
      calls.add(new int[] { g, s });
      return rows(4);
    });
    assertThat(short1).hasSize(4);
    assertThat(calls).hasSize(1);

    // a full answer is returned as it is even when it also met stale candidates
    calls.clear();
    SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(2, 3, 1_000, (g, s, stale) -> {
      calls.add(new int[] { g, s });
      stale.add(new RID(1, 1));
      return rows(6);
    });
    assertThat(calls).hasSize(1);
  }

  @Test
  void staleTopUpWidensBothBudgetsByTheStaleCountUntilTheAnswerIsFull() {
    final List<int[]> calls = new ArrayList<>();
    final ArrayList<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(2, 3, 10_000, (g, s, stale) -> {
      calls.add(new int[] { g, s });
      if (calls.size() == 1) {
        // two stale candidates cost the answer two rows
        stale.add(new RID(1, 1));
        stale.add(new RID(1, 2));
        return rows(4);
      }
      return rows(6);
    });
    assertThat(answer).hasSize(6);
    assertThat(calls).hasSize(2);
    // each stale record needs at most one more group and one more member per group
    assertThat(calls.get(1)).containsExactly(4, 5);
  }

  @Test
  void staleTopUpStopsWhenAWiderPassGivesNothingNew() {
    final List<int[]> calls = new ArrayList<>();
    final RID stale1 = new RID(1, 1);
    final ArrayList<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(2, 3, 100_000, (g, s, stale) -> {
      calls.add(new int[] { g, s });
      // a stale entry that never goes away (a restore leftover): met again by every pass, never yielding more rows
      stale.add(stale1);
      return rows(4);
    });
    assertThat(answer).hasSize(4);
    assertThat(calls).hasSize(2);
  }

  @Test
  void staleTopUpKeepsWideningWhileNewStaleCandidatesAreMetButNeverPastTheBudget() {
    final List<int[]> calls = new ArrayList<>();
    final int[] counter = { 0 };
    final int maxFetch = 2_000;
    final ArrayList<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(2, 3, maxFetch, (g, s, stale) -> {
      calls.add(new int[] { g, s });
      // steady churn: every pass meets a record deleted while it ran, and answers no more than the pass before
      stale.add(new RID(2, counter[0]++));
      return rows(4);
    });
    assertThat(answer).hasSize(4);
    assertThat(calls.size()).isGreaterThan(2);
    for (final int[] call : calls)
      assertThat(SQLFunctionVectorAbstract.groupedFetchBudget(call[0], call[1])).isLessThanOrEqualTo(maxFetch);
    // the widening at least doubles both budgets when a pass did not grow, so the passes grow with the log of the budget
    assertThat(calls.size()).isLessThan(12);
    for (int i = 2; i < calls.size(); i++) {
      assertThat(calls.get(i)[0]).isGreaterThanOrEqualTo(2 * calls.get(i - 1)[0]);
      assertThat(calls.get(i)[1]).isGreaterThanOrEqualTo(2 * calls.get(i - 1)[1]);
    }
  }

  @Test
  void staleTopUpDoesNotWidenPastTheCandidateBudget() {
    final List<int[]> calls = new ArrayList<>();
    // 2 groups of 3 asks for 30 candidates; one stale record widens to 3 x 4 = 60 candidates, which is over the cap of 59
    final ArrayList<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(2, 3, 59, (g, s, stale) -> {
      calls.add(new int[] { g, s });
      stale.add(new RID(1, calls.size()));
      return rows(5);
    });
    assertThat(answer).hasSize(5);
    assertThat(calls).hasSize(1);

    // the same widening fits a cap of 60
    calls.clear();
    SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(2, 3, 60, (g, s, stale) -> {
      calls.add(new int[] { g, s });
      if (calls.size() == 1)
        stale.add(new RID(1, 1));
      return rows(calls.size() == 1 ? 5 : 6);
    });
    assertThat(calls).hasSize(2);
    assertThat(calls.get(1)).containsExactly(3, 4);
  }

  @Test
  void staleTopUpReturnsTheLargestAnswerAndTheFresherOnATie() {
    final ArrayList<Object> older = rows(4);
    final ArrayList<Object> fresher = rows(4);
    final ArrayList<Object> smaller = rows(3);
    final ArrayList<ArrayList<Object>> passes = new ArrayList<>(List.of(older, fresher, smaller));
    final int[] n = { 0 };
    final ArrayList<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 3, 100_000, (g, s, stale) -> {
      final int i = n[0]++;
      // every pass meets a stale record of its own, so the loop only ends once the three answers were seen
      stale.add(new RID(5, i));
      return passes.get(Math.min(i, passes.size() - 1));
    });
    // the pass that answered fewer rows than an earlier one does not replace it; the tie went to the later pass
    assertThat(answer).isSameAs(fresher);
  }

  @Test
  void partitionHintNarrowsTheAllowedBucketsOfTheTypeItWasDerivedFor() {
    database.getSchema().createDocumentType("HintedParent");
    database.getSchema().createDocumentType("HintedChild").addSuperType("HintedParent");
    database.getSchema().createDocumentType("HintedPlain");

    final IntHashSet allowed = new IntHashSet();
    allowed.add(10);
    allowed.add(11);
    allowed.add(12);
    final IntHashSet hint = new IntHashSet();
    hint.add(11);
    hint.add(99);

    // the planner stashed no hint: the allow-list is returned as it is
    final CommandContext none = new BasicCommandContext().setDatabase(database);
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(allowed, "HintedPlain", none)).isSameAs(allowed);

    final CommandContext hinted = new BasicCommandContext().setDatabase(database);
    hinted.setVariable(CommandContext.PARTITION_PRUNED_TYPE_NAME_VAR, "HintedPlain");
    hinted.setVariable(CommandContext.PARTITION_PRUNED_BUCKET_FILE_IDS_VAR, hint);

    // a different target type than the one the hint was derived from
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(allowed, "HintedParent", hinted)).isSameAs(allowed);

    // the hint's bucket that the allow-list does not hold does not widen it, and the input is not mutated
    final IntHashSet narrowed = SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(allowed, "HintedPlain", hinted);
    assertThat(narrowed).isNotSameAs(allowed);
    assertThat(narrowed.toArray()).containsExactly(11);
    assertThat(allowed.size()).isEqualTo(3);

    // a hint that shares no bucket narrows to nothing: no record of the allow-list can be in the pruned partition
    final IntHashSet disjoint = new IntHashSet();
    disjoint.add(500);
    final CommandContext disjointCtx = new BasicCommandContext().setDatabase(database);
    disjointCtx.setVariable(CommandContext.PARTITION_PRUNED_TYPE_NAME_VAR, "HintedPlain");
    disjointCtx.setVariable(CommandContext.PARTITION_PRUNED_BUCKET_FILE_IDS_VAR, disjoint);
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(allowed, "HintedPlain", disjointCtx).isEmpty())
        .isTrue();

    // an empty hint is no hint
    final CommandContext emptyHint = new BasicCommandContext().setDatabase(database);
    emptyHint.setVariable(CommandContext.PARTITION_PRUNED_TYPE_NAME_VAR, "HintedPlain");
    emptyHint.setVariable(CommandContext.PARTITION_PRUNED_BUCKET_FILE_IDS_VAR, new IntHashSet());
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(allowed, "HintedPlain", emptyHint)).isSameAs(allowed);

    // the allow-list of a type with sub-types covers the sub-types' buckets, which the hint (the FROM type's own) does not
    final CommandContext parentHint = new BasicCommandContext().setDatabase(database);
    parentHint.setVariable(CommandContext.PARTITION_PRUNED_TYPE_NAME_VAR, "HintedParent");
    parentHint.setVariable(CommandContext.PARTITION_PRUNED_BUCKET_FILE_IDS_VAR, hint);
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(allowed, "HintedParent", parentHint)).isSameAs(allowed);

    // nothing to narrow, or nothing to narrow by
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(null, "HintedPlain", hinted)).isNull();
    final IntHashSet empty = new IntHashSet();
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(empty, "HintedPlain", hinted)).isSameAs(empty);
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(allowed, null, hinted)).isSameAs(allowed);
    assertThat(SQLFunctionVectorAbstract.narrowAllowedBucketIdsByPartitionHint(allowed, "HintedPlain", null)).isSameAs(allowed);
  }

  private static ArrayList<Object> rows(final int count) {
    final ArrayList<Object> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      rows.add(i);
    return rows;
  }
}
