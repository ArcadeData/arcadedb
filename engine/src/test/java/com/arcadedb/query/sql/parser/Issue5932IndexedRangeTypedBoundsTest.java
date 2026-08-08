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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5932. {@code LSMTreeIndexCursor}'s constructor computes properly-typed bounds via
 * {@code index.convertKeys(...)} into a local variable ({@code serializedFromKeys}) or a field
 * ({@code serializedToKeys}), but several internal consistency checks compared already-typed, page- or
 * transaction-overlay-stored keys against the RAW, unconverted {@code fromKeys}/{@code toKeys} fields instead -
 * e.g. an indexed {@code INTEGER} column compared against the literal {@code String} {@code "15"} rather than the
 * {@code Integer} {@code 15} the index actually stores. {@link com.arcadedb.serializer.BinaryComparator#compare}
 * trusts the caller that a value's runtime class matches its declared type and does an unguarded numeric cast, so
 * the mismatch surfaced as a {@code ClassCastException} instead of a meaningful comparison.
 * <p>
 * Two independent call sites hit this, exercised separately below: the page-cursor bootstrap (reachable once the
 * range's matching row is already committed) and {@code getClosestEntryInTx} (reachable purely through an open,
 * uncommitted transaction, with no committed page at all - {@link TestHelper#executeInNewDatabase} runs its whole
 * callback inside one transaction, so a query issued before an explicit {@code commit()} only ever sees the
 * in-transaction overlay).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5932IndexedRangeTypedBoundsTest {

  @Test
  void lessThanNumericStringOnIndexedIntegerColumnAfterCommitReturnsCorrectRow() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932LtCommitted", db -> {
      db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
      db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      db.newDocument("V").set("n", 10).save();
      db.newDocument("V").set("n", 20).save();
      db.commit();
      db.begin();

      try (final ResultSet rs = db.query("sql", "select n from V where n < '15'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void greaterThanNumericStringOnIndexedIntegerColumnAfterCommitReturnsCorrectRow() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932GtCommitted", db -> {
      db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
      db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      db.newDocument("V").set("n", 10).save();
      db.newDocument("V").set("n", 20).save();
      db.commit();
      db.begin();

      try (final ResultSet rs = db.query("sql", "select n from V where n > '15'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(20);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * The planner falls back to a full bucket scan with a row-level filter for this shape (a BETWEEN with
   * non-index-typed bounds), so this does not currently exercise {@code LSMTreeIndexCursor} - it is a plain
   * correctness guard, kept alongside the indexed-path regressions above for the same query shape.
   */
  @Test
  void betweenNumericStringBoundsOnIndexedIntegerColumnAfterCommitReturnsCorrectRow() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932BetweenCommitted", db -> {
      db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
      db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      db.newDocument("V").set("n", 10).save();
      db.newDocument("V").set("n", 20).save();
      db.newDocument("V").set("n", 30).save();
      db.commit();
      db.begin();

      try (final ResultSet rs = db.query("sql", "select n from V where n between '15' and '25'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(20);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * A fractional numeric String ({@code "15.5"}) is not a valid {@code Integer.parseInt} argument, so on an
   * INTEGER column it is rejected upstream (by {@code FetchFromIndexStep.valuesConvertToIndexKeyTypes}) before ever
   * reaching the index. A DOUBLE column accepts it via {@code Double.parseDouble}, so the mismatched-but-convertible
   * bound reaches {@code LSMTreeIndexCursor} unconverted, hitting {@code BinaryComparator}'s
   * {@code TYPE_DOUBLE}/{@code TYPE_FLOAT} branch's unguarded {@code (Number) value1} cast the same way the
   * INTEGER case hits the narrow-integral branch.
   */
  @Test
  void fractionalStringBoundOnIndexedDoubleColumnAfterCommitReturnsCorrectRow() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932FractionalDouble", db -> {
      db.getSchema().createDocumentType("V").createProperty("d", Type.DOUBLE);
      db.command("sql", "CREATE INDEX ON V (d) NOTUNIQUE");
      db.newDocument("V").set("d", 10.0).save();
      db.newDocument("V").set("d", 20.0).save();
      db.commit();
      db.begin();

      try (final ResultSet rs = db.query("sql", "select d from V where d < '15.5'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Double>getProperty("d")).isEqualTo(10.0);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * Reachable with NO committed page at all: the matching row is inserted in the same still-open transaction as
   * the query (as every {@link TestHelper#executeInNewDatabase} callback runs, absent an explicit intermediate
   * {@code commit()}), so the only candidate data lives in the {@code TransactionIndexContext} overlay navigated by
   * {@code LSMTreeIndexCursor.getClosestEntryInTx}.
   */
  @Test
  void lessThanNumericStringOnIndexedIntegerColumnWithinOpenTransactionReturnsCorrectRow() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932LtUncommitted", db -> {
      db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
      db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      db.newDocument("V").set("n", 10).save();
      db.newDocument("V").set("n", 20).save();

      // NOTE: no commit() here - the query below runs against the still-open transaction's overlay only.
      try (final ResultSet rs = db.query("sql", "select n from V where n < '15'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * Guard against over-fixing: correctly-typed bounds, mixed committed and in-transaction data, must keep returning
   * exactly the right rows once the raw/serialized inconsistency is resolved.
   */
  @Test
  void validComparisonsAcrossCommittedAndUncommittedDataStillWork() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932ValidMixed", db -> {
      db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
      db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      db.newDocument("V").set("n", 10).save();
      db.commit();
      db.begin();
      db.newDocument("V").set("n", 20).save();
      db.newDocument("V").set("n", 30).save();

      try (final ResultSet rs = db.query("sql", "select n from V where n > 15 order by n")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(20);
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(30);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * Code review follow-up on PR #5961: {@code LSMTreeIndexAbstract.convertKeysToDeclaredTypes()} must apply the
   * same case-insensitive collation folding as {@code convertKeys()} - page- and transaction-overlay-stored keys
   * of a {@code COLLATE CI} property are always lowercased, so a range bound compared against them has to be
   * folded too, or a differently-cased literal silently miscompares by plain byte order instead of throwing.
   */
  @Test
  void caseInsensitiveIndexedStringRangeQueryWithDifferentlyCasedBoundReturnsCorrectRow() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932CaseInsensitiveRange", db -> {
      db.getSchema().createDocumentType("V").createProperty("name", Type.STRING);
      db.command("sql", "CREATE INDEX ON V (name COLLATE CI) NOTUNIQUE");
      db.newDocument("V").set("name", "Alpha").save();
      db.newDocument("V").set("name", "Bravo").save();
      db.newDocument("V").set("name", "Charlie").save();
      db.commit();
      db.begin();

      try (final ResultSet rs = db.query("sql", "select name from V where name > 'bravo'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("Charlie");
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = db.query("sql", "select name from V where name < 'BRAVO'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alpha");
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * Code review follow-up on PR #5961: unlike the {@code BETWEEN} test above (whose planner falls back to a
   * bucket scan for that shape), an {@code AND} of two range conditions plans as a single indexed range scan
   * with both bounds set, exercising {@code typedFromKeys} and {@code typedToKeys} simultaneously through
   * {@code LSMTreeIndexCursor}.
   */
  @Test
  void compoundAndRangeConditionOnIndexedIntegerColumnUsesBothBoundsThroughIndexCursor() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932CompoundAndRange", db -> {
      db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
      db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      db.newDocument("V").set("n", 10).save();
      db.newDocument("V").set("n", 20).save();
      db.newDocument("V").set("n", 30).save();
      db.commit();
      db.begin();

      try (final ResultSet rs = db.query("sql", "select n from V where n > '5' and n < '25'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(20);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }
}
