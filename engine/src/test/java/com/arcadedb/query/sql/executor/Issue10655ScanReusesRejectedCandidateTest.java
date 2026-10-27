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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10655 item 2: a filtered scan used to build a new {@link ResultInternal} for every record it read, before the
 * WHERE clause had looked at it, so a row the clause rejected cost a Result that nothing ever used. A rejected
 * candidate is now pointed at the next record instead, and only a row the clause accepts leaves the scan as its own
 * object.
 */
class Issue10655ScanReusesRejectedCandidateTest extends TestHelper {

  private static final String TYPE    = "Scan10655";
  private static final String PROBE   = "probe10655";
  private static final int    RECORDS = 600;
  // EVERY 50TH RECORD MATCHES: grp = 0
  private static final int    GROUPS  = 50;
  private static final int    MATCHES = RECORDS / GROUPS;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1);
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).set("grp", i % GROUPS).set("tag", "a,b," + i).save();
    });
  }

  /**
   * The WHERE clause sees one candidate object for every run of rejected records, not one per record: the probe records
   * the {@code $current} each evaluation ran against, and the scan may only allocate a new one after it handed the
   * previous one downstream as a match (plus one per unit of a parallel scan).
   */
  @Test
  void rejectedCandidatesShareOneRow() {
    final Map<Object, Boolean> seen = Collections.synchronizedMap(new IdentityHashMap<>());
    final List<RID> evaluated = Collections.synchronizedList(new ArrayList<>());
    final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract(PROBE) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        final Object current = context.getVariable("current");
        seen.put(current, Boolean.TRUE);
        evaluated.add(((Result) current).getIdentity().get());
        return true;
      }

      @Override
      public String getSyntax() {
        return PROBE + "()";
      }
    });
    try {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE " + PROBE + "() = true AND grp = 0")) {
        while (rs.hasNext())
          rows.add(rs.next());
      }

      assertThat(rows).hasSize(MATCHES);
      assertThat(evaluated).as("the WHERE clause ran once per record, each time on its own record").hasSize(RECORDS);
      assertThat(new HashSet<>(evaluated)).hasSize(RECORDS);
      // ONE CANDIDATE PER MATCH HANDED DOWNSTREAM, PLUS THE ONE THE TRAILING REJECTED RUN SHARES; BEFORE THE FIX: ONE PER RECORD
      assertThat(seen.size()).as("candidate rows the WHERE clause saw for %d records", RECORDS).isLessThanOrEqualTo(MATCHES + 1);
    } finally {
      sql.getFunctionFactory().unregister(PROBE);
    }
  }

  /**
   * A row the clause accepted is never the object the scan reuses: after every row has been read, each one still answers
   * its own record, though the scan rejected dozens of records between two matches.
   */
  @Test
  void acceptedRowsKeepTheirOwnRecord() {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE grp = 7")) {
      while (rs.hasNext())
        rows.add(rs.next());
    }

    assertThat(rows).hasSize(MATCHES);
    final Set<Integer> ids = new HashSet<>();
    final Map<Result, Boolean> distinct = new IdentityHashMap<>();
    for (final Result row : rows) {
      assertThat((Integer) row.getProperty("grp")).isEqualTo(7);
      final Document element = row.getElement().get();
      assertThat(element.getInteger("grp")).isEqualTo(7);
      ids.add(row.getProperty("id"));
      distinct.put(row, Boolean.TRUE);
    }
    assertThat(ids).hasSize(MATCHES);
    assertThat(distinct).hasSize(MATCHES);
  }

  /** {@code $current} and a method call on a field read the record under evaluation, not one the candidate showed earlier. */
  @Test
  void currentResolvesToTheRecordUnderEvaluation() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE $current.grp = 3")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo((long) MATCHES);
    }
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + TYPE + " WHERE tag.split(',')[2] = '123'")) {
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(123);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** A row something wrote to is never pointed at another record: what was written belongs to the record it was written on. */
  @Test
  void aWrittenRowIsNotRewrapped() {
    database.transaction(() -> {
      final List<Document> docs = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE id < 2 ORDER BY id")) {
        while (rs.hasNext())
          docs.add(rs.next().getElement().get());
      }

      final ResultInternal plain = new ResultInternal(docs.get(0));
      assertThat(plain.rewrapElement(docs.get(1))).isTrue();
      assertThat(plain.getElement().get().getIdentity()).isEqualTo(docs.get(1).getIdentity());

      final ResultInternal withMetadata = new ResultInternal(docs.get(0));
      withMetadata.setMetadata("score", 1);
      assertThat(withMetadata.rewrapElement(docs.get(1))).isFalse();
      assertThat(withMetadata.getElement().get().getIdentity()).isEqualTo(docs.get(0).getIdentity());

      final ResultInternal withTemporary = new ResultInternal(docs.get(0));
      withTemporary.setTemporaryProperty("t", 1);
      assertThat(withTemporary.rewrapElement(docs.get(1))).isFalse();

      final ResultInternal withRemoval = new ResultInternal(docs.get(0));
      withRemoval.removeProperty("grp");
      assertThat(withRemoval.rewrapElement(docs.get(1))).isFalse();

      final ResultInternal withContent = new ResultInternal(database);
      withContent.setElement(docs.get(0));
      assertThat(withContent.rewrapElement(docs.get(1))).isFalse();
    });
  }

  /**
   * A correlated subquery in the WHERE clause reads the outer row through {@code $parent.$current}, and runs a filtered
   * scan of its own, with its own reused candidate, while the outer candidate is being evaluated.
   */
  @Test
  void correlatedSubqueryReadsTheOuterRecordUnderEvaluation() {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT id FROM " + TYPE + " WHERE id IN (SELECT id FROM " + TYPE + " WHERE id = $parent.$current.id AND grp = 5) ORDER BY id")) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    final List<Integer> expected = new ArrayList<>();
    for (int i = 5; i < RECORDS; i += GROUPS)
      expected.add(i);
    assertThat(ids).isEqualTo(expected);
  }

  /**
   * A parallel filtered scan runs one copy of the step per unit of pages, each on one worker at a time, so each copy keeps
   * its own rejected candidate: the rows it returns are the sequential scan's, each answering its own record.
   */
  @Test
  void parallelScanReturnsTheSequentialRowsEachWithItsOwnRecord() {
    final Object pagesPerUnit = database.getConfiguration().getValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    try {
      scanInParallel();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, pagesPerUnit);
    }
  }

  private void scanInParallel() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Par10655", 1);
      final String padding = "x".repeat(200);
      for (int i = 0; i < 20_000; i++)
        database.newDocument("Par10655").set("id", i).set("grp", i % GROUPS).set("pad", padding).save();
    });
    final String query = "SELECT FROM Par10655 WHERE grp = 9";
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("(parallel)");
    }

    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    final List<Integer> ids = new ArrayList<>();
    final Map<Result, Boolean> distinct = new IdentityHashMap<>();
    for (final Result row : rows) {
      assertThat(row.getElement().get().getInteger("grp")).isEqualTo(9);
      ids.add(row.getElement().get().getInteger("id"));
      distinct.put(row, Boolean.TRUE);
    }
    final List<Integer> expected = new ArrayList<>();
    for (int i = 9; i < 20_000; i += GROUPS)
      expected.add(i);
    assertThat(ids).isEqualTo(expected);
    assertThat(distinct).hasSize(expected.size());
  }

  /**
   * Every field of a row besides its element is checked by {@code rewrapElement}: a field added later that carries row
   * state and is not checked would be moved to another record by a filtered scan. Each field is set on its own, to a
   * non-null value of its type, and the row must then refuse to be re-pointed.
   */
  @Test
  void everyFieldBesidesTheElementBlocksTheRewrap() throws Exception {
    final DocumentType type = database.getSchema().getType(TYPE);
    final Document first;
    final Document second;
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE id < 2 ORDER BY id")) {
      first = rs.next().getElement().get();
      second = rs.next().getElement().get();
    }

    int checked = 0;
    for (Class<?> c = ResultInternal.class; c != Object.class; c = c.getSuperclass()) {
      for (final Field field : c.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) || field.getName().equals("element"))
          continue;
        final ResultInternal row = new ResultInternal(first);
        field.setAccessible(true);
        field.set(row, sampleOf(field.getType(), type));
        assertThat(row.rewrapElement(second)).as("a row whose '%s' is set must not be re-pointed", field.getName()).isFalse();
        ++checked;
      }
    }
    assertThat(checked).isGreaterThanOrEqualTo(9);
  }

  private Object sampleOf(final Class<?> fieldType, final DocumentType type) {
    if (fieldType == Map.class)
      return new HashMap<>();
    if (fieldType == Set.class)
      return new HashSet<>();
    if (fieldType == DocumentType.class)
      return type;
    if (fieldType.isInstance(database))
      return database;
    if (fieldType == Object.class)
      return "value";
    throw new AssertionError("no sample value for a field of type " + fieldType.getName() + ": extend sampleOf() and rewrapElement()");
  }
}
