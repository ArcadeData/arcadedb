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
import com.arcadedb.query.sql.antlr.SQLASTBuilder;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.schema.Type;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL comparison, containment and null-coalescing operators.
 * <p>
 * A container for the regression suites of the feature; each nested class keeps its own fixture.
 */
class SqlComparisonOperatorTest {
  /**
   * Regression for #5900: A range comparison (, =, BETWEEN) between a numeric column and a non-numeric String literal (e.g.
   * <p>
   * Regression test for issue #5900. A range comparison ({@code <}, {@code >}, {@code <=}, {@code >=}, {@code BETWEEN})
   * between a numeric column and a non-numeric String literal (e.g. {@code WHERE n < 'abc'} on an {@code INTEGER}
   * column) escaped as a raw {@link NumberFormatException} from {@link Type#convert}, crashing the query instead of
   * completing. Equality already handled this gracefully ({@link QueryOperatorEquals} catches the conversion failure
   * and answers "not equal"); the range operators and BETWEEN did not have the same guard. A comparison across
   * incompatible types has no defined ordering, so - consistent with equality and with the Cypher engine's
   * established behaviour for the analogous case (#5225) - it must answer "no match" rather than throw.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ComparisonTypeMismatchTest {

    @Test
    void lessThanNonNumericStringOnIntegerColumnDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900LtInt", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.newDocument("V").set("n", 10).save();

        assertThatCode(() -> {
          try (final ResultSet rs = db.query("sql", "select n from V where n < 'abc'")) {
            assertThat(rs.hasNext()).isFalse();
          }
        }).doesNotThrowAnyException();
      });
    }

    @Test
    void greaterThanNonNumericStringOnIntegerColumnDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900GtInt", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.newDocument("V").set("n", 10).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n > 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    @Test
    void lessOrEqualAndGreaterOrEqualNonNumericStringDoNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900LeGeInt", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.newDocument("V").set("n", 10).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n <= 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n >= 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    @Test
    void betweenWithNonNumericBoundDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900Between", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.newDocument("V").set("n", 10).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n between 1 and 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n between 'abc' and 100")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    @Test
    void shortAndByteTypedPropertiesAlsoDoNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900ShortByte", db -> {
        final var type = db.getSchema().createDocumentType("V");
        type.createProperty("s", Type.SHORT);
        type.createProperty("b", Type.BYTE);
        db.newDocument("V").set("s", (short) 10).set("b", (byte) 5).save();

        try (final ResultSet rs = db.query("sql", "select s from V where s < 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select b from V where b > 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    /**
     * Guard against over-fixing: a numeric-valued String must still parse and compare correctly.
     */
    @Test
    void validNumericStringComparisonsStillWork() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900ValidStringCompare", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.newDocument("V").set("n", 10).save();
        db.newDocument("V").set("n", 20).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n < '15'")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    /**
     * Guard against over-fixing: an INT column compared against a LONG literal outside int range must still widen
     * correctly and return the right rows (this already worked before the fix, per the issue's own report).
     */
    @Test
    void intColumnComparedAgainstOutOfRangeLongLiteralStillCorrect() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900IntVsLong", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.newDocument("V").set("n", 10).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n < 2147483648")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    /**
     * Code review follow-up on PR #5922: an indexed column bypasses the row-scan operators entirely and pushes the
     * bound straight into {@code FetchFromIndexStep}, which reached the index's own unguarded
     * {@code convertKeys()}/{@code Type.convert()} - the identical crash through a very common trigger (any indexed
     * comparison column), untouched by the row-scan fix above.
     */
    @Test
    void lessThanNonNumericStringOnIndexedIntegerColumnDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900LtIndexed", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
        db.newDocument("V").set("n", 10).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n < 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n > 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n <= 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n >= 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    @Test
    void equalsNonNumericStringOnIndexedIntegerColumnDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900EqIndexed", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
        db.newDocument("V").set("n", 10).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n = 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    /**
     * Code review follow-up on PR #5922: {@code createCursor()} returning {@code null} for a failed conversion is
     * only safe if every caller checks for it. The multi-value branch of {@code processInCondition()} stored the
     * per-item cursor unconditionally and wrapped it in a sub-iterator that called {@code hasNext()}/{@code close()}
     * on it without a null check, so a mixed-type IN list against an indexed column traded the original
     * {@code NumberFormatException} for a {@code NullPointerException} instead of actually fixing the crash.
     */
    @Test
    void inListWithNonNumericItemOnIndexedIntegerColumnDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900InIndexed", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
        db.newDocument("V").set("n", 10).save();
        db.newDocument("V").set("n", 20).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n in [10, 'abc']")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n in ['abc', 'def']")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    @Test
    void betweenWithNonNumericBoundOnIndexedIntegerColumnDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900BetweenIndexed", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
        db.newDocument("V").set("n", 10).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n between 1 and 'abc'")) {
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n between 'abc' and 100")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    /**
     * Guard against over-fixing on the indexed path: valid, correctly-typed comparisons must still use the index
     * and return the correct rows. Uses a typed {@code 15} rather than the numeric string {@code '15'} used by the
     * non-indexed equivalent above: a numeric-string bound against an indexed column hits a separate, pre-existing
     * bug where {@code LSMTreeIndexCursor}'s constructor compares the raw (unconverted) bound against an
     * already-typed stored key and throws {@code ClassCastException} - reproduces identically on unmodified
     * {@code main}, independent of this fix, and out of scope here (filed separately).
     */
    @Test
    void validComparisonsOnIndexedIntegerColumnStillWork() throws Exception {
      TestHelper.executeInNewDatabase("testIssue5900ValidIndexed", db -> {
        db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
        db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
        db.newDocument("V").set("n", 10).save();
        db.newDocument("V").set("n", 20).save();

        try (final ResultSet rs = db.query("sql", "select n from V where n < 15")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n = 10")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select n from V where n between 5 and 15")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(10);
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }
  }

  /**
   * Regression for #5932: LSMTreeIndexCursor's constructor computes properly-typed bounds via index.convertKeys(...) into a local...
   * <p>
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
  @Nested
  class IndexedRangeTypedBoundsTest {

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

        try (final ResultSet rs = db.query("sql", "select name from V where name.toLowerCase() > 'bravo'")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<String>getProperty("name")).isEqualTo("Charlie");
          assertThat(rs.hasNext()).isFalse();
        }
        try (final ResultSet rs = db.query("sql", "select name from V where name.toLowerCase() < 'bravo'")) {
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

  /**
   * Regression for #10758: CONTAINSALL expanded a byte[] right-hand operand into its bytes, so a list holding a BINARY value did not...
   * <p>
   * #10758: {@code CONTAINSALL} expanded a {@code byte[]} right-hand operand into its bytes, so a list holding a BINARY value did not
   * contain all of it, where {@code CONTAINS} and {@code CONTAINSANY} found it. The right-hand side of a containment operator is the
   * value searched FOR, and a {@code byte[]} is ArcadeDB's BINARY scalar: one value, never a sequence.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ContainsBinaryOperandTest extends TestHelper {

    private static final byte[] BINARY = { 1, 2 };

    @Test
    void everyContainmentOperatorFindsABinaryValueInAList() {
      database.command("sql", "CREATE DOCUMENT TYPE Blob").close();
      database.transaction(() -> database.newDocument("Blob").set("vals", new ArrayList<>(List.of(new byte[] { 1, 2 }))).save());

      for (final String operator : new String[] { "CONTAINS", "CONTAINSANY", "CONTAINSALL" }) {
        assertThat(count("SELECT count(*) AS n FROM Blob WHERE vals " + operator + " :b", Map.of("b", BINARY))).as(operator)
            .isEqualTo(1L);
        assertThat(count("SELECT count(*) AS n FROM Blob WHERE vals " + operator + " :b", Map.of("b", List.of(BINARY)))).as(
            operator + " [binary]").isEqualTo(1L);
        // the bytes of the value are no elements of the list
        assertThat(count("SELECT count(*) AS n FROM Blob WHERE vals " + operator + " :b", Map.of("b", List.of((byte) 1, (byte) 2))))
            .as(operator + " [bytes]").isEqualTo(0L);
      }
    }

    /**
     * The census: every containment condition of the parser package with a value form ({@code execute(Object, Object)}) reads a
     * {@code byte[]} operand, alone or as the one element of a list, as {@code CONTAINS} reads it. A condition added later is in the
     * census without anyone listing it.
     */
    @Test
    void everyContainmentConditionReadsAByteArrayOperandAsOneValue() throws Exception {
      final List<Class<?>> conditions = containmentConditions();
      assertThat(conditions).extracting(Class::getSimpleName)
          .contains("ContainsCondition", "ContainsAnyCondition", "ContainsAllCondition");

      final ContainsCondition reference = new ContainsCondition();
      final List<Object> container = List.of(new byte[] { 1, 2 });
      final List<Object> containerOfBytes = List.of((byte) 1, (byte) 2);

      for (final Class<?> type : conditions) {
        final Object condition = type.getDeclaredConstructor().newInstance();
        final Method execute = type.getMethod("execute", Object.class, Object.class);

        for (final Object operand : new Object[] { BINARY, List.of(BINARY) }) {
          assertThat(execute.invoke(condition, container, operand)).as(type.getSimpleName() + " over [binary]")
              .isEqualTo(reference.execute(container, operand)).isEqualTo(true);
          assertThat(execute.invoke(condition, containerOfBytes, operand)).as(type.getSimpleName() + " over [bytes]")
              .isEqualTo(reference.execute(containerOfBytes, operand)).isEqualTo(false);
        }
      }
    }

    private static List<Class<?>> containmentConditions() throws Exception {
      // the directory the parser classes were loaded from, not the first one of the package on the class path: the test classes of
      // this very package come first
      final URL root = ContainsCondition.class.getProtectionDomain().getCodeSource().getLocation();
      final File packageDir = new File(new File(root.toURI()), ContainsCondition.class.getPackageName().replace('.', File.separatorChar));
      final File[] files = packageDir.listFiles((dir, name) -> name.startsWith("Contains") && name.endsWith("Condition.class"));
      assertThat(files).isNotNull();

      final List<Class<?>> conditions = new ArrayList<>();
      for (final File file : files) {
        final String name = file.getName();
        final Class<?> type = Class.forName(
            ContainsCondition.class.getPackageName() + "." + name.substring(0, name.length() - ".class".length()));
        try {
          type.getMethod("execute", Object.class, Object.class);
          conditions.add(type);
        } catch (final NoSuchMethodException e) {
          // no value form: a condition over a text or a map, not over a sequence
        }
      }
      return conditions;
    }

    private long count(final String sql, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("sql", sql, params)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Regression for #6393: The SQL null-coalescing operator ?? used to return its RIGHT operand whatever the left one was, because the...
   * <p>
   * The SQL null-coalescing operator {@code ??} used to return its RIGHT operand whatever the left one was, because the
   * ANTLR AST builder had no visitor for the {@code nullCoalescing} alternative: the default {@code visitChildren}
   * returned the last child it visited, so the left operand never reached the tree - issue #6393.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NullCoalescingTest extends TestHelper {

    @Test
    void leftOperandWinsWhenNotNull() {
      try (final ResultSet rs = database.query("sql", "SELECT 'left' ?? 'right' AS n")) {
        assertThat(rs.next().<String>getProperty("n")).isEqualTo("left");
      }
      try (final ResultSet rs = database.query("sql", "SELECT 1 ?? 2 AS n")) {
        assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(1);
      }
    }

    @Test
    void rightOperandWinsWhenLeftIsNull() {
      try (final ResultSet rs = database.query("sql", "SELECT null ?? 'right' AS n")) {
        assertThat(rs.next().<String>getProperty("n")).isEqualTo("right");
      }
    }

    @Test
    void bothOperandsNullYieldsNull() {
      try (final ResultSet rs = database.query("sql", "SELECT null ?? null AS n")) {
        assertThat(rs.next().<Object>getProperty("n")).isNull();
      }
    }

    @Test
    void missingPropertyFallsBackToTheDefault() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc6393");
        database.command("sql", "INSERT INTO Doc6393 SET name = 'Jay'");
        database.command("sql", "INSERT INTO Doc6393 SET other = 'x'");
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT name ?? 'unknown' AS n FROM Doc6393 ORDER BY n")) {
          assertThat(rs.next().<String>getProperty("n")).isEqualTo("Jay");
          assertThat(rs.next().<String>getProperty("n")).isEqualTo("unknown");
        }
      });
    }

    @Test
    void chainedCoalescingTakesTheFirstNonNull() {
      try (final ResultSet rs = database.query("sql", "SELECT null ?? 'b' ?? 'c' AS n")) {
        assertThat(rs.next().<String>getProperty("n")).isEqualTo("b");
      }
    }

    @Test
    void usableInAWhereClause() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Where6393");
        database.command("sql", "INSERT INTO Where6393 SET id = 1, name = 'Jay'");
        database.command("sql", "INSERT INTO Where6393 SET id = 2");
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT id FROM Where6393 WHERE (name ?? 'unknown') = 'unknown'")) {
          assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(2);
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    /**
     * The short-circuit is a behaviour, not just an optimisation: the right operand may be a function call or a
     * sub-query, and it must not run when the fallback is not taken. Observed with an argument that throws when it IS
     * evaluated, so the two directions are told apart by whether the error escapes.
     */
    @Test
    void rightOperandIsNotEvaluatedWhenTheLeftIsNotNull() {
      try (final ResultSet rs = database.query("sql", "SELECT 1 ?? 'abcdef'.substring('boom') AS n")) {
        assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(1);
      }
      // ...and IS evaluated when the fallback is taken, so the test above cannot pass by the operand being dead.
      assertThatThrownBy(() -> database.query("sql", "SELECT null ?? 'abcdef'.substring('boom') AS n").next())
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void renderingKeepsTheLeftOperand() {
      assertThat(renderProjection("SELECT a ?? b AS n FROM V")).contains("a ?? b");
      // `||` binds tighter than `??`, so the right operand is the whole concatenation.
      assertThat(renderProjection("SELECT a ?? b || c AS n FROM V")).contains("a ?? b || c");
    }

    @Test
    void renderedFormReparsesToTheSameTree() {
      for (final String query : new String[] { "SELECT a ?? b AS n FROM V", "SELECT a ?? b || c AS n FROM V",
          "SELECT a || b ?? c AS n FROM V", "SELECT (a ?? b) AS n FROM V", "SELECT a ?? b ?? c AS n FROM V",
          "SELECT a + 1 ?? b AS n FROM V", "SELECT (a ?? b) + 1 AS n FROM V" }) {
        final String rendered = render(query);
        final String reRendered = render(rendered);
        assertThat(reRendered).as("round trip of '%s'", query).isEqualTo(rendered);
      }
    }

    private String renderProjection(final String query) {
      return render(query);
    }

    private static String render(final String query) {
      final SQLLexer lexer = new SQLLexer(CharStreams.fromString(query));
      final SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
      return new SQLASTBuilder().visitParse(parser.parse()).toString();
    }
  }
}
