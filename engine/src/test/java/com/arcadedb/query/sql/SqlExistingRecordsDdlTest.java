/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlExistingRecordsDdlTest {
  /**
   * Regression for #7143.
   * <p>
   * Regression test for issue #7143 (second item): {@code CREATE ... IF NOT EXISTS} answered one row the first
   * time and ZERO rows on a retry, so a client that checks the row count to confirm success read the retry as a
   * failure - which is exactly what the guard exists to prevent. {@code CREATE INDEX}, {@code CREATE TRIGGER}
   * and {@code CREATE GRAPH ANALYTICAL VIEW} already answered one row carrying a {@code created} flag; every
   * guarded CREATE now follows that convention.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreateIfNotExistsRowShape extends TestHelper {

    @Test
    void everyGuardedCreateAnswersOneRowWithACreatedFlagOnBothCalls() {
      assertCreatedThenIdempotent("create document type Doc1 if not exists", "typeName", "Doc1");
      assertCreatedThenIdempotent("create vertex type Vert1 if not exists", "typeName", "Vert1");
      assertCreatedThenIdempotent("create edge type Edge1 if not exists", "typeName", "Edge1");
      assertCreatedThenIdempotent("create bucket Bucket1 if not exists", "bucketName", "Bucket1");
      assertCreatedThenIdempotent("create property Doc1.name if not exists string", "propertyName", "name");
      assertCreatedThenIdempotent(
          "create timeseries type Series1 if not exists timestamp ts tags (sensor string) fields (value double)",
          "typeName", "Series1");
      assertCreatedThenIdempotent("create index if not exists on Doc1 (name) unique", "name", "Doc1[name]");
    }

    /**
     * Runs {@code statement} twice and asserts both calls answer exactly one row naming the same object, the
     * first with {@code created=true} and the second with {@code created=false}.
     */
    private void assertCreatedThenIdempotent(final String statement, final String nameProperty,
        final String expectedName) {
      final Result first = single(statement);
      assertThat(first.<Boolean>getProperty("created")).as("%s (first call)", statement).isTrue();
      assertThat(first.<Object>getProperty(nameProperty)).as("%s (first call)", statement).isEqualTo(expectedName);

      final Result second = single(statement);
      assertThat(second.<Boolean>getProperty("created")).as("%s (retry)", statement).isFalse();
      assertThat(second.<Object>getProperty(nameProperty)).as("%s (retry)", statement).isEqualTo(expectedName);
      assertThat(second.<Object>getProperty("operation")).as("%s (retry)", statement)
          .isEqualTo(first.getProperty("operation"));
    }

    private Result single(final String statement) {
      try (final ResultSet rs = database.command("sql", statement)) {
        final List<Result> rows = rs.stream().toList();
        assertThat(rows).as("row count of: %s", statement).hasSize(1);
        return rows.getFirst();
      }
    }
  }

  /**
   * Regression for #8943.
   * <p>
   * Regression test for issue #8943: {@code ALTER PROPERTY ... MANDATORY/NOTNULL true} did not look at the records already
   * written, so a record without the property stayed legal, stayed out of the index, and the planner (which trusts the
   * constraint to mean "the index holds every record") silently dropped it from an index-ordered {@code ORDER BY}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AlterPropertyExistingRecords extends TestHelper {

    private void load() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.v INTEGER");
      database.command("sql", "CREATE INDEX ON T (v) NOTUNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < 4; i++)
          database.newDocument("T").set("id", i).set("v", i).save();
        database.newDocument("T").set("id", 4).save();
      });
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        long n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        return n;
      }
    }

    @Test
    void mandatoryIsRefusedWhenARecordLacksTheProperty() {
      load();
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.v MANDATORY true"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("T.v").hasMessageContaining("mandatory");
      assertThat(database.getSchema().getType("T").getProperty("v").isMandatory()).isFalse();
    }

    @Test
    void notNullIsRefusedWhenARecordHoldsAnExplicitNull() {
      database.command("sql", "CREATE DOCUMENT TYPE N");
      database.command("sql", "CREATE PROPERTY N.v INTEGER");
      database.transaction(() -> database.newDocument("N").set("v", (Object) null).save());
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY N.v NOTNULL true"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("cannot be null");
      assertThat(database.getSchema().getType("N").getProperty("v").isNotNull()).isFalse();
    }

    @Test
    void orderByStillReturnsEveryRecordAfterTheRefusedAlter() {
      load();
      // NOTNULL alone only rejects an explicit null, so it is accepted; MANDATORY is the one refused
      database.command("sql", "ALTER PROPERTY T.v NOTNULL true");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.v MANDATORY true"))
          .isInstanceOf(CommandExecutionException.class);
      assertThat(count("SELECT id FROM T")).isEqualTo(5);
      assertThat(count("SELECT FROM T ORDER BY v")).isEqualTo(5);
    }

    @Test
    void alterIsAcceptedWhenEveryRecordConforms() {
      load();
      database.transaction(() -> database.command("sql", "UPDATE T SET v = 99 WHERE v IS NULL"));
      database.command("sql", "ALTER PROPERTY T.v NOTNULL true");
      database.command("sql", "ALTER PROPERTY T.v MANDATORY true");
      assertThat(database.getSchema().getType("T").getProperty("v").isMandatory()).isTrue();
      assertThat(count("SELECT FROM T ORDER BY v")).isEqualTo(5);
    }

    @Test
    void turningTheConstraintOffIsNeverRefused() {
      load();
      database.command("sql", "ALTER PROPERTY T.v MANDATORY false");
      database.command("sql", "ALTER PROPERTY T.v NOTNULL false");
      assertThat(database.getSchema().getType("T").getProperty("v").isMandatory()).isFalse();
      assertThat(database.getSchema().getType("T").getProperty("v").isNotNull()).isFalse();
    }

    @Test
    void reapplyingAConstraintThatIsAlreadySetDoesNotRescan() {
      database.command("sql", "CREATE DOCUMENT TYPE R");
      database.command("sql", "CREATE PROPERTY R.v INTEGER");
      database.command("sql", "ALTER PROPERTY R.v MANDATORY true");
      // a record written while the constraint is on cannot violate it; the repeat must simply succeed
      database.transaction(() -> database.newDocument("R").set("v", 1).save());
      database.command("sql", "ALTER PROPERTY R.v MANDATORY true");
      assertThat(database.getSchema().getType("R").getProperty("v").isMandatory()).isTrue();
    }

    @Test
    void theRefusalIsRaisedInsideADdlScriptToo() {
      load();
      assertThatThrownBy(() -> database.command("sqlscript",
          "CREATE DOCUMENT TYPE Other;\nALTER PROPERTY T.v MANDATORY true;"))
          .isInstanceOf(CommandExecutionException.class);
      assertThat(database.getSchema().getType("T").getProperty("v").isMandatory()).isFalse();
    }

    @Test
    void aSubtypeRecordIsChecked() {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE PROPERTY P.v INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
      database.transaction(() -> database.newDocument("C").set("other", 1).save());
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY P.v MANDATORY true"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("#").hasMessageContaining("is mandatory");
    }
  }

  /**
   * Regression for #9112.
   * <p>
   * Regression test for #9112 (merged #9016, #9017): CREATE PROPERTY, ALTER PROPERTY MIN/MAX/REGEXP and the openCypher
   * existence constraints did not look at the records already stored, so a record that violated the new declaration stayed
   * in the database (unable to take any update, or silently missing from an index-ordered read).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExistingRecordsConstraints extends TestHelper {

    private void insert(final String type, final String setClause) {
      database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET " + setClause).close());
    }

    private boolean hasProperty(final String type, final String property) {
      return database.getSchema().getType(type).existsProperty(property);
    }

    @Test
    void createPropertyRefusesStringOverInteger() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = 'abc'");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("T.v").hasMessageContaining("abc");
      assertThat(hasProperty("T", "v")).isFalse();
    }

    @Test
    void createPropertyRefusesBooleanOverInteger() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = true");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class);
      assertThat(hasProperty("T", "v")).isFalse();
    }

    @Test
    void createPropertyRefusesUnreadableDate() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = 'not a date'");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v DATETIME")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(hasProperty("T", "v")).isFalse();
    }

    @Test
    void createPropertyAcceptsConvertibleValues() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = '12'");
      insert("T", "v = 7");
      insert("T", "id = 1");
      database.command("sql", "CREATE PROPERTY T.v INTEGER");
      assertThat(hasProperty("T", "v")).isTrue();
      database.command("sql", "CREATE INDEX ON T (v) NOTUNIQUE");
    }

    @Test
    void createPropertyOverSubtypeRecords() {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
      insert("C", "v = 'abc'");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY P.v INTEGER")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(hasProperty("P", "v")).isFalse();
    }

    @Test
    void createPropertyRefusesMandatoryNotNullOverRecordWithoutValue() {
      database.command("sql", "CREATE DOCUMENT TYPE C");
      insert("C", "id = 1, v = 10");
      insert("C", "id = 2");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY C.v INTEGER (mandatory true, notnull true)")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("C.v");
      assertThat(hasProperty("C", "v")).isFalse();
    }

    @Test
    void createPropertyRefusesNotNullOverNull() {
      database.command("sql", "CREATE DOCUMENT TYPE N");
      insert("N", "id = 1, v = null");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY N.v INTEGER (notnull true)")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(hasProperty("N", "v")).isFalse();
    }

    @Test
    void createPropertyRefusesMinMaxRegexpViolations() {
      database.command("sql", "CREATE DOCUMENT TYPE M");
      insert("M", "id = 1, a = 5, b = 25, c = 'ABC'");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY M.a INTEGER (min 10)")).isInstanceOf(
          CommandExecutionException.class);
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY M.b INTEGER (max 20)")).isInstanceOf(
          CommandExecutionException.class);
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY M.c STRING (regexp '[a-z]+')")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(hasProperty("M", "a") || hasProperty("M", "b") || hasProperty("M", "c")).isFalse();
    }

    @Test
    void createPropertyOverEmptyTypeAndConformingRecords() {
      database.command("sql", "CREATE DOCUMENT TYPE E");
      database.command("sql", "CREATE PROPERTY E.v INTEGER (mandatory true, notnull true, min 1, max 9)");
      insert("E", "v = 3");
      database.command("sql", "CREATE DOCUMENT TYPE F");
      insert("F", "v = 3, s = 'abc'");
      database.command("sql", "CREATE PROPERTY F.v INTEGER (mandatory true, notnull true, min 1, max 9)");
      database.command("sql", "CREATE PROPERTY F.s STRING (regexp '[a-z]+')");
      assertThat(hasProperty("F", "s")).isTrue();
    }

    @Test
    void alterPropertyMinMaxRegexpAreRefusedOverViolatingRecords() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a INTEGER");
      database.command("sql", "CREATE PROPERTY T.b INTEGER");
      database.command("sql", "CREATE PROPERTY T.c STRING");
      insert("T", "a = 5, b = 25, c = 'ABC'");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.a MIN 10")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("T.a");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.b MAX 20")).isInstanceOf(CommandExecutionException.class);
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.c REGEXP '[a-z]+'")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(database.getSchema().getType("T").getProperty("a").getMin()).isNull();
      assertThat(database.getSchema().getType("T").getProperty("b").getMax()).isNull();
      assertThat(database.getSchema().getType("T").getProperty("c").getRegexp()).isNull();
      // the same bounds are fine when the records conform
      database.command("sql", "ALTER PROPERTY T.a MIN 1");
      database.command("sql", "ALTER PROPERTY T.b MAX 30");
      database.command("sql", "ALTER PROPERTY T.c REGEXP '[A-Z]+'");
      // and clearing a bound never needs a scan
      database.command("sql", "ALTER PROPERTY T.a MIN null");
    }

    @Test
    void cypherNotNullConstraintIsRefusedOverNodeWithoutProperty() {
      database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, name: 'a'}), (:P {id: 2})"));
      assertThatThrownBy(
          () -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:P) REQUIRE n.name IS NOT NULL")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("name");
      assertThat(database.getSchema().getType("P").existsProperty("name") && database.getSchema().getType("P").getProperty("name")
          .isMandatory()).isFalse();
      // the node can still be updated
      database.transaction(() -> database.command("opencypher", "MATCH (n:P {id: 2}) SET n.note = 'x'"));
    }

    @Test
    void cypherNodeKeyConstraintIsRefusedOverNodeWithoutProperty() {
      database.transaction(() -> database.command("opencypher", "CREATE (:Q {id: 1, name: 'a'}), (:Q {id: 2})"));
      assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:Q) REQUIRE n.name IS NODE KEY")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("name");
      assertThat(database.getSchema().getType("Q").getIndexesByProperties("name")).isNullOrEmpty();
      database.transaction(() -> database.command("opencypher", "MATCH (n:Q {id: 2}) SET n.note = 'x'"));
    }

    @Test
    void cypherConstraintsOverConformingNodesStillWork() {
      database.transaction(() -> database.command("opencypher", "CREATE (:R {id: 1, name: 'a'}), (:R {id: 2, name: 'b'})"));
      database.command("opencypher", "CREATE CONSTRAINT FOR (n:R) REQUIRE n.name IS NOT NULL");
      assertThat(database.getSchema().getType("R").getProperty("name").isMandatory()).isTrue();
    }

    @Test
    void refusedCreatePropertyCanBeRetriedAfterFixingTheData() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = 'abc'");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class);
      assertThat(hasProperty("T", "v")).isFalse();
      database.transaction(() -> database.command("sql", "UPDATE T SET v = 5").close());
      database.command("sql", "CREATE PROPERTY T.v INTEGER");
      assertThat(hasProperty("T", "v")).isTrue();
    }

    @Test
    void clearingABoundNeedsNoScan() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a INTEGER (min 1)");
      insert("T", "a = 5");
      assertThat(database.command("sql", "ALTER PROPERTY T.a MIN null").hasNext()).isTrue();
    }

    @Test
    void invalidRegexpIsRefusedEvenOverAnEmptyType() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.s STRING");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.s REGEXP '[a-'")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("Invalid regular expression");
    }

    @Test
    void createPlainStringPropertyOverExistingRecords() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "s = 'abc'");
      database.command("sql", "CREATE PROPERTY T.s STRING");
      assertThat(hasProperty("T", "s")).isTrue();
    }

    /** Issue #10204: an index on a STRING property compares stored numbers as text, so a stored number is refused too. */
    @Test
    void createPlainStringPropertyOverStoredNumbersIsRefused() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "s = 5");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.s STRING")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("STRING");
      assertThat(hasProperty("T", "s")).isFalse();
    }

    @Test
    void createPropertyIfNotExistsOverExistingDataIsANoOp() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.v INTEGER");
      insert("T", "v = 1");
      database.command("sql", "CREATE PROPERTY T.v IF NOT EXISTS STRING");
      assertThat(database.getSchema().getType("T").getProperty("v").getType().name()).isEqualTo("INTEGER");
    }

    @Test
    void alterPropertyOnAParentTypeSeesViolatingSubtypeRecords() {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE PROPERTY P.v INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
      insert("C", "v = 5");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY P.v MIN 10")).isInstanceOf(CommandExecutionException.class);
      assertThat(database.getSchema().getType("P").getProperty("v").getMin()).isNull();
    }

    @Test
    void createPropertyRefusesALossyNarrowing() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = 3000000000");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("3000000000");
      assertThat(hasProperty("T", "v")).isFalse();
      database.command("sql", "CREATE PROPERTY T.v LONG");
    }

    @Test
    void malformedBoundIsRefusedOverAnEmptyType() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a INTEGER");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.a MAX 'abc'")).isInstanceOf(CommandExecutionException.class);
      assertThat(database.getSchema().getType("T").getProperty("a").getMax()).isNull();
    }

    @Test
    void cypherCompositeNodeKeyIsRefusedWhenOnlyTheSecondPropertyIsMissing() {
      database.transaction(() -> database.command("opencypher", "CREATE (:K {a: 1, b: 2}), (:K {a: 3})"));
      assertThatThrownBy(
          () -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:K) REQUIRE (n.a, n.b) IS NODE KEY")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("b");
    }

    @Test
    void aFailingAttributeLeavesNoPropertyBehind() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = 5");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER (mandatory true, min 10)")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(hasProperty("T", "v")).isFalse();
    }

    @Test
    void createPropertyRefusesNaNOverAnIntegralType() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.transaction(() -> database.newDocument("T").set("v", Double.NaN).save());
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class);
      assertThat(hasProperty("T", "v")).isFalse();
    }

    @Test
    void createPropertyOverBooleanAcceptsBooleanLikeValues() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = true");
      insert("T", "v = 1");
      database.command("sql", "CREATE PROPERTY T.v BOOLEAN");
      assertThat(hasProperty("T", "v")).isTrue();
    }

    @Test
    void createPropertyRefusesAnOverflowingStringOverInteger() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = '3000000000'");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(CommandExecutionException.class);
      assertThat(hasProperty("T", "v")).isFalse();
    }

    @Test
    void createListOfIntegerIsRefusedOverANestedListElement() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.transaction(() -> database.command("sql", "INSERT INTO T SET l = [[1, 2], 3]").close());
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.l LIST OF INTEGER")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(hasProperty("T", "l")).isFalse();
    }

    @Test
    void createListOfLongAcceptsStoredIntegers() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.transaction(() -> database.command("sql", "INSERT INTO T SET l = [1, 2, 3]").close());
      database.command("sql", "CREATE PROPERTY T.l LIST OF LONG");
      assertThat(hasProperty("T", "l")).isTrue();
    }

    @Test
    void nonNumericLengthBoundOnAStringIsRefusedOverAnEmptyType() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.s STRING");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.s MAX 'abc'")).isInstanceOf(
          CommandExecutionException.class);
      assertThat(database.getSchema().getType("T").getProperty("s").getMax()).isNull();
    }

    @Test
    void preparedCreatePropertyKeepsItsTypeNameAfterARefusal() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      insert("T", "v = 'abc'");
      for (int i = 0; i < 2; i++)
        assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.v INTEGER")).isInstanceOf(
            CommandExecutionException.class).hasMessageContaining("T.v");
    }
  }
}
