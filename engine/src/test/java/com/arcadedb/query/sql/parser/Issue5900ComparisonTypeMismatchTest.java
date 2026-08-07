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
import static org.assertj.core.api.Assertions.assertThatCode;

/**
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
class Issue5900ComparisonTypeMismatchTest {

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
