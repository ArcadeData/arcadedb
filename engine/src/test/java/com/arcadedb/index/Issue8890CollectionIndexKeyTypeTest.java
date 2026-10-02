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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A BY ITEM list index was keyed as STRING whatever the declared item type, so {@code CONTAINS 7} missed a stored
 * {@code 7.0} that the scan finds, and {@code CONTAINSVALUE} on a map compared with {@code Map.containsValue}, missing
 * the other way (issue #8890). The indexed and the not indexed property must answer alike.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8890CollectionIndexKeyTypeTest extends TestHelper {
  private int typeCounter = 0;

  private long count(final String sql, final Object... args) {
    return database.query("sql", sql, args).stream().count();
  }

  private void assertIndexedAndScanAgree(final String listType, final Object stored, final long expected, final Object... operands) {
    final String type = "L" + typeCounter++;
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".a " + listType);
    database.command("sql", "CREATE PROPERTY " + type + ".b " + listType);
    database.command("sql", "CREATE INDEX ON " + type + " (a BY ITEM) NOTUNIQUE");
    database.transaction(() -> database.newDocument(type).set("a", new ArrayList<>(List.of(stored)), "b", new ArrayList<>(List.of(stored))).save());

    for (final Object operand : operands) {
      assertThat(count("SELECT FROM " + type + " WHERE a CONTAINS ?", operand)).as("%s indexed, operand %s (%s)", listType, operand,
          operand.getClass().getSimpleName()).isEqualTo(expected);
      assertThat(count("SELECT FROM " + type + " WHERE b CONTAINS ?", operand)).as("%s scan, operand %s (%s)", listType, operand,
          operand.getClass().getSimpleName()).isEqualTo(expected);
    }
  }

  @Test
  void listOfDoubleFoundByAnyNumber() {
    assertIndexedAndScanAgree("LIST OF DOUBLE", 7.0, 1, 7, 7L, 7.0, new BigDecimal("7"), "7");
  }

  @Test
  void listOfIntegerAndLongFoundByADouble() {
    assertIndexedAndScanAgree("LIST OF INTEGER", 7, 1, 7, 7L, 7.0, new BigDecimal("7.0"));
    assertIndexedAndScanAgree("LIST OF LONG", 7L, 1, 7, 7L, 7.0, new BigDecimal("7.0"));
  }

  @Test
  void listOfDecimalFoundByADouble() {
    assertIndexedAndScanAgree("LIST OF DECIMAL", new BigDecimal("19.90"), 1, new BigDecimal("19.9"), 19.9d);
  }

  @Test
  void listOfDatetimeFoundByTheSameMoment() {
    final Date moment = new Date(1_790_858_096_789L);
    assertIndexedAndScanAgree("LIST OF DATETIME", moment, 1, moment, new Date(moment.getTime()));
  }

  @Test
  void aValueNotInTheListIsStillNotFound() {
    assertIndexedAndScanAgree("LIST OF DOUBLE", 7.0, 0, 8, 8.5, "abc");
  }

  @Test
  void listOfStringAndUntypedListKeepWorking() {
    assertIndexedAndScanAgree("LIST OF STRING", "x", 1, "x");
    assertIndexedAndScanAgree("LIST", "x", 1, "x");
  }

  @Test
  void indexedKeyTypeIsTheDeclaredItemType() {
    database.command("sql", "CREATE DOCUMENT TYPE K");
    database.command("sql", "CREATE PROPERTY K.a LIST OF DOUBLE");
    database.command("sql", "CREATE PROPERTY K.u LIST");
    database.command("sql", "CREATE INDEX ON K (a BY ITEM) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON K (u BY ITEM) NOTUNIQUE");
    assertThat(database.getSchema().getType("K").getPolymorphicIndexByProperties("a by item").getKeyTypes())
        .containsExactly(Type.DOUBLE);
    assertThat(database.getSchema().getType("K").getPolymorphicIndexByProperties("u by item").getKeyTypes())
        .containsExactly(Type.STRING);
  }

  @Test
  void mapContainsValueAnswersTheSameWithAndWithoutIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE M");
    database.command("sql", "CREATE PROPERTY M.m MAP OF DOUBLE");
    database.command("sql", "CREATE PROPERTY M.n MAP OF DOUBLE");
    database.command("sql", "CREATE INDEX ON M (m BY VALUE) NOTUNIQUE");
    database.transaction(() -> database.newDocument("M").set("m", new HashMap<>(Map.of("x", 7.0)), "n", new HashMap<>(Map.of("x", 7.0))).save());

    for (final Object operand : new Object[] { 7.0, 7, 7L, "7" }) {
      assertThat(count("SELECT FROM M WHERE m CONTAINSVALUE ?", operand)).as("indexed %s", operand).isEqualTo(1);
      assertThat(count("SELECT FROM M WHERE n CONTAINSVALUE ?", operand)).as("scan %s", operand).isEqualTo(1);
    }
    for (final Object operand : new Object[] { 8, 8.5, "abc" })
      assertThat(count("SELECT FROM M WHERE n CONTAINSVALUE ?", operand)).as("scan %s", operand).isEqualTo(0);
  }

  @Test
  void rangeLookupsByAnUnreadableKeyAnswerAsTheScanDoes() {
    database.command("sql", "CREATE DOCUMENT TYPE W");
    database.command("sql", "CREATE PROPERTY W.i INTEGER");
    database.command("sql", "CREATE PROPERTY W.j INTEGER");
    database.command("sql", "CREATE INDEX ON W (i) NOTUNIQUE");
    database.transaction(() -> database.newDocument("W").set("i", 7, "j", 7).save());
    for (final String op : new String[] { "=", "<", "<=", ">", ">=", "<>" })
      assertThat(count("SELECT FROM W WHERE i " + op + " ?", "7.0")).as(op).isEqualTo(count("SELECT FROM W WHERE j " + op + " ?", "7.0"));
    assertThat(count("SELECT FROM W WHERE i BETWEEN ? AND ?", "6.0", "8.0"))
        .isEqualTo(count("SELECT FROM W WHERE j BETWEEN ? AND ?", "6.0", "8.0"));
    assertThat(count("SELECT FROM W WHERE i IN ?", List.of("7.0", "x"))).isEqualTo(count("SELECT FROM W WHERE j IN ?", List.of("7.0", "x")));
  }

  @Test
  void theIndexApiAnswersNoRowForAnUnreadableKey() {
    database.command("sql", "CREATE DOCUMENT TYPE Z");
    database.command("sql", "CREATE PROPERTY Z.i INTEGER");
    database.command("sql", "CREATE INDEX ON Z (i) NOTUNIQUE");
    database.transaction(() -> database.newDocument("Z").set("i", 7).save());
    final Index index = database.getSchema().getType("Z").getPolymorphicIndexByProperties("i");
    assertThat(index.get(new Object[] { "7.0" }).hasNext()).isFalse();
    assertThat(index.get(new Object[] { "7" }).hasNext()).isTrue();
  }

  @Test
  void aListItemOfAnotherTypeIsStillIndexed() {
    database.command("sql", "CREATE DOCUMENT TYPE X");
    database.command("sql", "CREATE PROPERTY X.a LIST OF INTEGER");
    database.command("sql", "CREATE INDEX ON X (a BY ITEM) NOTUNIQUE");
    database.transaction(() -> database.newDocument("X").set("a", new ArrayList<>(List.of(7, 8L, "9"))).save());
    assertThat(count("SELECT FROM X WHERE a CONTAINS 7")).isEqualTo(1);
    assertThat(count("SELECT FROM X WHERE a CONTAINS 9")).isEqualTo(1);
  }

  @Test
  void aListItemThatCannotBeReadAsTheDeclaredTypeIsRefusedWithOrWithoutTheIndex() {
    // the declared OF type is enforced when the record is saved, before any index sees the item: typing the BY ITEM key
    // narrows nothing that could be written before
    database.command("sql", "CREATE DOCUMENT TYPE Y");
    database.command("sql", "CREATE PROPERTY Y.a LIST OF INTEGER");
    database.command("sql", "CREATE PROPERTY Y.b LIST OF INTEGER");
    database.command("sql", "CREATE INDEX ON Y (a BY ITEM) NOTUNIQUE");
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Y").set("a", new ArrayList<>(List.of(7, "abc"))).save()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Y").set("b", new ArrayList<>(List.of(7, "abc"))).save()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rebuiltIndexKeepsTheDeclaredKeyType() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.a LIST OF DOUBLE");
    database.command("sql", "CREATE INDEX ON R (a BY ITEM) NOTUNIQUE");
    database.transaction(() -> database.newDocument("R").set("a", new ArrayList<>(List.of(7.0, 8.5))).save());
    database.command("sql", "REBUILD INDEX *");
    assertThat(count("SELECT FROM R WHERE a CONTAINS 7")).isEqualTo(1);
    assertThat(count("SELECT FROM R WHERE a CONTAINS 8.5")).isEqualTo(1);
  }
}
