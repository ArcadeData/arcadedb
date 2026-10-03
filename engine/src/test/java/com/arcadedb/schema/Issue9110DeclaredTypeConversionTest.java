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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9110 (consolidates #9014, #9015, #9041, #9027, #9024): a value the declared property type cannot hold must be
 * refused with an error naming the property, never stored as NULL, as 0, as the wrong type or as a wrapped number.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9110DeclaredTypeConversionTest extends TestHelper {

  private void assertRefused(final String typeName, final String property, final Object value) {
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(typeName).set(property, value).save()))
        .as("%s <- %s", property, value == null ? null : value.getClass().getSimpleName() + " " + value)
        .isInstanceOf(IllegalArgumentException.class);
  }

  private Object roundTrip(final String typeName, final String property, final Object value) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set(property, value).save().getIdentity());
    final Document d = database.lookupByRID(rid[0], true).asDocument();
    return d.get(property);
  }

  @Test
  void inconvertibleValueIsNeverStoredAsNullInANumericProperty() {
    final DocumentType type = database.getSchema().createDocumentType("Num9110");
    final String[] props = { "b", "s", "i", "l", "f", "d" };
    final Type[] types = { Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG, Type.FLOAT, Type.DOUBLE };
    for (int i = 0; i < props.length; i++)
      type.createProperty(props[i], types[i]);

    final Object[] bad = { true, new ArrayList<>(List.of(1, 2)), new HashMap<>(Map.of("a", 1)), LocalDate.of(2026, 10, 3) };
    for (final String p : props)
      for (final Object v : bad) {
        // a date written to a LONG is the epoch milliseconds, as documented for DATE -> LONG
        if (p.equals("l") && v instanceof LocalDate)
          continue;
        assertRefused("Num9110", p, v);
      }
  }

  @Test
  void errorNamesTheProperty() {
    database.getSchema().createDocumentType("Named9110").createProperty("age", Type.INTEGER);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Named9110").set("age", true).save()))
        .hasStackTraceContaining("age");
  }

  @Test
  void sqlAndCypherRefuseInconvertibleValues() {
    database.getSchema().createVertexType("T9110").createProperty("integer", Type.INTEGER);
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T9110 SET integer = true").close()))
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T9110 SET integer = [1, 2]").close()))
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("opencypher", "CREATE (:T9110 {integer: true})").close()))
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("opencypher", "CREATE (:T9110 {integer: [1, 2]})").close()))
        .isInstanceOf(RuntimeException.class);
    assertThat(database.countType("T9110", true)).isZero();
  }

  @Test
  void emptyStringIsRefusedForEveryNumericType() {
    final DocumentType type = database.getSchema().createDocumentType("Empty9110");
    final String[] props = { "b", "s", "i", "l", "f", "d", "dec" };
    final Type[] types = { Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG, Type.FLOAT, Type.DOUBLE, Type.DECIMAL };
    for (int i = 0; i < props.length; i++)
      type.createProperty(props[i], types[i]);
    for (final String p : props)
      assertRefused("Empty9110", p, "");
  }

  @Test
  void longRefusesValuesOutsideTheSixtyFourBitRange() {
    database.getSchema().createDocumentType("Long9110").createProperty("l", Type.LONG);
    final Object[] bad = { new BigInteger("12345678901234567890"), new BigDecimal("1E+20"), 1.0e19d, 1.0e19f, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, -1.0e19d, 9.223372036854775807E18d };
    for (final Object v : bad)
      assertRefused("Long9110", "l", v);

    assertThat(roundTrip("Long9110", "l", Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    assertThat(roundTrip("Long9110", "l", Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
    assertThat(roundTrip("Long9110", "l", new BigInteger("9223372036854775807"))).isEqualTo(Long.MAX_VALUE);
    assertThat(roundTrip("Long9110", "l", -9.223372036854775808E18d)).isEqualTo(Long.MIN_VALUE);
    assertThat(roundTrip("Long9110", "l", 42.9d)).isEqualTo(42L);
  }

  @Test
  void numberToBooleanIsFalseOnlyForZero() {
    database.getSchema().createDocumentType("Bool9110").createProperty("flag", Type.BOOLEAN);
    assertThat(roundTrip("Bool9110", "flag", 0)).isEqualTo(false);
    assertThat(roundTrip("Bool9110", "flag", 0.0d)).isEqualTo(false);
    assertThat(roundTrip("Bool9110", "flag", BigDecimal.ZERO)).isEqualTo(false);
    assertThat(roundTrip("Bool9110", "flag", 1)).isEqualTo(true);
    assertThat(roundTrip("Bool9110", "flag", 2)).isEqualTo(true);
    assertThat(roundTrip("Bool9110", "flag", 0.5d)).isEqualTo(true);
    assertThat(roundTrip("Bool9110", "flag", -0.5d)).isEqualTo(true);
    assertThat(roundTrip("Bool9110", "flag", 4294967296L)).isEqualTo(true);
    assertThat(roundTrip("Bool9110", "flag", 1e10d)).isEqualTo(true);
    assertThat(roundTrip("Bool9110", "flag", "TRUE")).isEqualTo(true);

    assertRefused("Bool9110", "flag", new ArrayList<>(List.of(1, 2)));
    assertRefused("Bool9110", "flag", LocalDate.of(2026, 10, 3));
  }

  @Test
  void booleanLookupByFractionMatchesTheTrueRecords() {
    database.getSchema().createDocumentType("BoolQ9110").createProperty("flag", Type.BOOLEAN);
    database.transaction(() -> {
      database.newDocument("BoolQ9110").set("flag", true).save();
      database.newDocument("BoolQ9110").set("flag", false).save();
    });
    assertThat(database.query("sql", "SELECT count(*) AS c FROM BoolQ9110 WHERE flag = 0.5").next().<Long>getProperty("c"))
        .isEqualTo(1L);
    assertThat(database.query("sql", "SELECT count(*) AS c FROM BoolQ9110 WHERE flag = 4294967296").next().<Long>getProperty("c"))
        .isEqualTo(1L);
    assertThat(database.query("sql", "SELECT count(*) AS c FROM BoolQ9110 WHERE flag = 0").next().<Long>getProperty("c"))
        .isEqualTo(1L);
  }

  @Test
  void typesWithoutACaseForTheValueRefuseItInsteadOfKeepingIt() {
    final DocumentType type = database.getSchema().createDocumentType("Keep9110");
    type.createProperty("dec", Type.DECIMAL);
    type.createProperty("dt", Type.DATETIME);
    type.createProperty("dtm", Type.DATETIME_MICROS);
    type.createProperty("bin", Type.BINARY);
    type.createProperty("link", Type.LINK);
    type.createProperty("str", Type.STRING);

    assertRefused("Keep9110", "dec", true);
    assertRefused("Keep9110", "dt", "");
    assertRefused("Keep9110", "dt", true);
    assertRefused("Keep9110", "dtm", new HashMap<>(Map.of("a", 1)));
    assertRefused("Keep9110", "bin", "abc");
    assertRefused("Keep9110", "link", "abc");
    assertRefused("Keep9110", "link", new ArrayList<>(List.of(1, 2)));
    assertRefused("Keep9110", "str", new byte[] { 1, 2, 3 });
    assertRefused("Keep9110", "str", new int[] { 1, 2, 3 });
  }

  @Test
  void byteArrayIsRefusedByEveryDeclaredTypeThatCannotHoldIt() {
    final DocumentType type = database.getSchema().createDocumentType("Bytes9110");
    type.createProperty("i", Type.INTEGER);
    type.createProperty("flag", Type.BOOLEAN);
    type.createProperty("dec", Type.DECIMAL);
    type.createProperty("dt", Type.DATETIME);
    for (final String p : new String[] { "i", "flag", "dec", "dt" })
      assertRefused("Bytes9110", p, new byte[] { 1, 2, 3 });
  }

  @Test
  void digitOnlyStringIsAnEpochCountForEveryDateTarget() {
    final DocumentType type = database.getSchema().createDocumentType("Epoch9110");
    type.createProperty("dt", Type.DATETIME);
    type.createProperty("d", Type.DATE);
    assertThat(roundTrip("Epoch9110", "dt", "1791000000000")).isEqualTo(roundTrip("Epoch9110", "dt", 1791000000000L));
    assertThat(roundTrip("Epoch9110", "dt", "1791000000000")).isNotNull();
  }

  @Test
  void crossJavaTimeShapesConvert() {
    final DocumentType type = database.getSchema().createDocumentType("Time9110");
    type.createProperty("dt", Type.DATETIME);
    type.createProperty("d", Type.DATE);
    final java.time.OffsetDateTime offset = java.time.OffsetDateTime.parse("2026-10-03T10:15:30+02:00");
    assertThat(roundTrip("Time9110", "dt", offset)).isNotNull();
    assertThat(roundTrip("Time9110", "dt", java.time.ZonedDateTime.parse("2026-10-03T10:15:30Z"))).isNotNull();
    assertThat(roundTrip("Time9110", "dt", java.time.Instant.parse("2026-10-03T10:15:30Z"))).isNotNull();
    assertThat(roundTrip("Time9110", "dt", LocalDate.of(2026, 10, 3))).isNotNull();
    assertThat(roundTrip("Time9110", "d", offset)).isNotNull();
    assertThat(roundTrip("Time9110", "d", java.time.ZonedDateTime.parse("2026-10-03T10:15:30Z"))).isNotNull();
    assertThat(Type.convert(database, offset, java.time.Instant.class)).isEqualTo(offset.toInstant());
    assertThat(Type.convert(database, java.time.LocalDateTime.of(2026, 10, 3, 10, 0), java.time.ZonedDateTime.class))
        .isNotNull();
    assertThat(Type.convert(database, java.time.LocalDateTime.of(2026, 10, 3, 10, 0), java.time.Instant.class)).isNotNull();
  }

  @Test
  void nanIsRefusedByABooleanProperty() {
    database.getSchema().createDocumentType("Nan9110").createProperty("flag", Type.BOOLEAN);
    assertRefused("Nan9110", "flag", Double.NaN);
  }

  @Test
  void gettersOnSchemalessDataKeepTheirLenientBehavior() {
    database.getSchema().createDocumentType("Loose9110");
    database.transaction(() -> {
      database.newDocument("Loose9110").set("a", new ArrayList<>(List.of(1, 2))).set("b", "").set("c", true).save();
    });
    final Document d = database.query("sql", "SELECT FROM Loose9110").next().getElement().get();
    assertThat(d.getInteger("a")).isNull();
    assertThat(d.getInteger("b")).isEqualTo(0);
    assertThat(d.getInteger("c")).isNull();
  }

  @Test
  void indexedIntegerLookupByBooleanDoesNotThrow() {
    final DocumentType type = database.getSchema().createDocumentType("Look9110");
    type.createProperty("i", Type.INTEGER);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "i");
    database.transaction(() -> database.newDocument("Look9110").set("i", 1).save());
    assertThat(database.query("sql", "SELECT FROM Look9110 WHERE i = true").hasNext()).isFalse();
  }

  @Test
  void validValuesStillConvert() {
    final DocumentType type = database.getSchema().createDocumentType("Ok9110");
    type.createProperty("dec", Type.DECIMAL);
    type.createProperty("bin", Type.BINARY);
    type.createProperty("str", Type.STRING);
    type.createProperty("link", Type.LINK);
    type.createProperty("i", Type.INTEGER);

    assertThat(roundTrip("Ok9110", "dec", "1.5")).isEqualTo(new BigDecimal("1.5"));
    assertThat(roundTrip("Ok9110", "dec", 2)).isEqualTo(new BigDecimal("2"));
    assertThat((byte[]) roundTrip("Ok9110", "bin", new byte[] { 1, 2 })).containsExactly(1, 2);
    assertThat(roundTrip("Ok9110", "str", 12)).isEqualTo("12");
    assertThat(roundTrip("Ok9110", "i", "12")).isEqualTo(12);
    assertThat(roundTrip("Ok9110", "i", 12L)).isEqualTo(12);
  }

  @Test
  void indexedAndPlainPropertiesAgree() {
    final DocumentType type = database.getSchema().createDocumentType("Idx9110");
    type.createProperty("flag", Type.BOOLEAN);
    type.createProperty("dec", Type.DECIMAL);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "flag");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "dec");
    assertRefused("Idx9110", "flag", new ArrayList<>(List.of(1, 2)));
    assertRefused("Idx9110", "dec", true);
    assertThat(database.countType("Idx9110", true)).isZero();
  }

  @Test
  void convertOrKeepStillKeepsTheOriginalForTheRemoteClient() {
    final List<Integer> list = new ArrayList<>(List.of(1, 2));
    assertThat(Type.convertOrKeep(database, list, Integer.class)).isSameAs(list);
    assertThat(Type.convertOrKeep(database, true, Integer.class)).isEqualTo(true);
    assertThat(Type.convertOrNull(database, true, Integer.class)).isNull();
  }
}
