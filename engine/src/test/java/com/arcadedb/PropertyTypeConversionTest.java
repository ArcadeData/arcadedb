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
package com.arcadedb;

import com.arcadedb.database.Binary;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PropertyTypeConversionTest {
  /**
   * Regression for #5091: Type.convert() wrapped an empty JSONArray into a singleton list.
   * {@code Type.convert()} wrapped an empty (or any) {@link JSONArray} value into a singleton
   * {@link List} containing the whole array instead of converting the array's elements. This happened
   * because {@link JSONArray} is an {@link Iterable} but not a {@link java.util.Collection}, so it fell
   * through to the {@code List.of(value)} branch.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EmptyArrayList extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE TreeItem");
        database.command("sql", "CREATE DOCUMENT TYPE Tree");
        database.command("sql", "CREATE PROPERTY Tree.items LIST OF TreeItem");
        database.command("sql", "CREATE VERTEX TYPE TreeTest");
        database.command("sql", "CREATE PROPERTY TreeTest.tree EMBEDDED OF Tree");
      });
    }

    @Test
    void setEmptyJSONArrayProducesEmptyList() {
      database.transaction(() -> {
        final JSONObject treeValue = new JSONObject();
        treeValue.put("@type", "Tree");
        treeValue.put("items", new JSONArray());

        final MutableDocument doc = database.newVertex("TreeTest");
        doc.set("tree", treeValue);

        final Object tree = doc.get("tree");
        assertThat(tree).isInstanceOf(EmbeddedDocument.class);

        final Object items = ((EmbeddedDocument) tree).get("items");
        assertThat(items).isInstanceOf(List.class);
        // The bug returned a singleton list holding the empty JSONArray as element 0.
        assertThat((List<?>) items).isEmpty();
      });
    }

    @Test
    void setNonEmptyJSONArrayCopiesElements() {
      database.transaction(() -> {
        final JSONArray itemsValue = new JSONArray();
        itemsValue.put(new JSONObject().put("@type", "TreeItem").put("name", "a"));
        itemsValue.put(new JSONObject().put("@type", "TreeItem").put("name", "b"));

        final JSONObject treeValue = new JSONObject();
        treeValue.put("@type", "Tree");
        treeValue.put("items", itemsValue);

        final MutableDocument doc = database.newVertex("TreeTest");
        doc.set("tree", treeValue);

        final Object items = ((EmbeddedDocument) doc.get("tree")).get("items");
        assertThat(items).isInstanceOf(List.class);
        // Two real elements, not one wrapped JSONArray.
        assertThat((List<?>) items).hasSize(2);
      });
    }

    @Test
    void sqlInsertStillProducesEmptyList() {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO TreeTest SET tree = {\"@type\": \"Tree\", \"items\": []}");
        final var rs = database.query("sql", "SELECT tree.items AS items FROM TreeTest");
        assertThat(rs.hasNext()).isTrue();
        final Object items = rs.next().getProperty("items");
        assertThat(items).isInstanceOf(List.class);
        assertThat((List<?>) items).isEmpty();
      });
    }

    @Test
    void typeConvertEmptyJSONArrayToListIsEmpty() {
      // Direct unit-level assertion on the conversion primitive at the heart of the bug.
      final Object converted = Type.convert(database, new JSONArray(), List.class);
      assertThat(converted).isInstanceOf(List.class);
      assertThat((List<?>) converted).isEmpty();

      final Object convertedCollection = Type.convert(database, new JSONArray(), Collection.class);
      assertThat(convertedCollection).isInstanceOf(Collection.class);
      assertThat((Collection<?>) convertedCollection).isEmpty();
    }
  }

  /**
   * Regression for #6061: conversions between lists and binary or float array properties.
   * {@link Type#convert} had narrowing branches from {@link java.util.Collection} to {@code float[]},
   * {@code double[]}, {@code int[]}, {@code long[]} and {@code short[]}, but not to {@code byte[]}. A
   * JSON array received over the wire (e.g. through {@code RemoteGraphBatch}, or any JSON-based write
   * path) for a BINARY property therefore parsed into a {@code List<Number>} that was silently stored
   * as an untyped {@link List} instead of being converted to {@code byte[]}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BinaryPropertyListConversion extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Blob");
        database.command("sql", "CREATE PROPERTY Blob.data BINARY");
      });
    }

    @Test
    void setListOnBinaryPropertyConvertsToByteArray() {
      database.transaction(() -> {
        final MutableDocument doc = database.newVertex("Blob");
        doc.set("data", List.of(1, 2, 3, 4));

        final Object data = doc.get("data");
        assertThat(data).isInstanceOf(byte[].class);
        assertThat((byte[]) data).containsExactly((byte) 1, (byte) 2, (byte) 3, (byte) 4);
      });
    }

    @Test
    void typeConvertListToByteArray() {
      final Object converted = Type.convert(database, List.of(1, 2, 3, 4), byte[].class);
      assertThat(converted).isInstanceOf(byte[].class);
      assertThat((byte[]) converted).containsExactly((byte) 1, (byte) 2, (byte) 3, (byte) 4);
    }

    @Test
    void typeConvertEmptyListToEmptyByteArray() {
      final Object converted = Type.convert(database, List.of(), byte[].class);
      assertThat(converted).isInstanceOf(byte[].class);
      assertThat((byte[]) converted).isEmpty();
    }

    // Code review follow-up: a null element in the source List/Collection previously NPE'd deep
    // inside narrowToIntegral()/the float/double narrowing branches instead of raising a clean
    // validation error. Not specific to byte[] - the same null guard now covers every
    // Collection -> primitive-array narrowing branch in Type.convert() (byte[]/short[]/int[]/long[]
    // via narrowToIntegral(), float[]/double[] via the new requireNonNullNumber() helper).
    @Test
    void typeConvertListWithNullElementToByteArrayThrowsCleanly() {
      final List<Integer> withNull = Arrays.asList(1, null, 3);
      assertThatThrownBy(() -> Type.convert(database, withNull, byte[].class))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("null element");
    }

    @Test
    void typeConvertListWithNullElementToFloatArrayThrowsCleanly() {
      final List<Double> withNull = Arrays.asList(1.0, null, 3.0);
      assertThatThrownBy(() -> Type.convert(database, withNull, float[].class))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("null element");
    }
  }

  /**
   * Regression for #9054: a String holding a lone UTF-16
   * surrogate was silently stored as '?', so another string came back and a UNIQUE index saw it equal to "?".
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LoneSurrogate extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE S");
        database.command("sql", "CREATE PROPERTY S.s STRING");
        database.command("sql", "CREATE DOCUMENT TYPE U");
        database.command("sql", "CREATE PROPERTY U.s STRING");
        database.command("sql", "CREATE INDEX ON U (s) UNIQUE");
      });
    }

    @Test
    void loneSurrogatesAreRefusedOnWrite() {
      for (final String value : new String[] { "\uD83D", "\uDE00", "a\uD83Db", "\uDE00\uD83D" })
        assertThatThrownBy(() -> database.transaction(() -> save("S", value))).isInstanceOf(ValidationException.class)
            .hasMessageContaining("lone UTF-16 surrogate").satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.VALIDATION));

      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM S")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
      }
    }

    @Test
    void wellFormedAndQuestionMarkStringsRoundTrip() {
      final String[] values = { "abc", "😀", "?", "a?b??", "😀?😀", "" };
      database.transaction(() -> {
        for (final String v : values)
          save("S", v);
      });
      try (final ResultSet rs = database.query("sql", "SELECT s FROM S")) {
        int count = 0;
        while (rs.hasNext()) {
          assertThat(values).contains(rs.next().<String>getProperty("s"));
          count++;
        }
        assertThat(count).isEqualTo(values.length);
      }
    }

    @Test
    void uniqueIndexDoesNotCollideLoneSurrogateWithQuestionMark() {
      database.transaction(() -> save("U", "?"));
      assertThatThrownBy(() -> database.transaction(() -> save("U", "\uD83D"))).isInstanceOf(ValidationException.class);
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM U")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
      }
    }

    @Test
    void binaryPutStringRefusesLoneSurrogate() {
      final Binary binary = new Binary();
      assertThatThrownBy(() -> binary.putString("\uD83D")).isInstanceOf(ValidationException.class);
      assertThatThrownBy(() -> binary.putString(0, "x\uDE00")).isInstanceOf(ValidationException.class);
      binary.putString("?");
      binary.position(0);
      assertThat(binary.getString()).isEqualTo("?");
    }

    private void save(final String type, final String value) {
      final MutableDocument d = database.newDocument(type);
      d.set("s", value);
      d.save();
    }
  }

  /**
   * Regression for #9111: the element validation of {@code LIST OF} / {@code MAP OF} refused every
   * value of a date type, and let anything through when the declared element type is a document type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ListOfDateAndDocumentTypes extends TestHelper {
    private static final LocalDateTime LDT = LocalDateTime.of(2026, 10, 3, 10, 20, 30, 123456000);

    private Object read(final RID rid, final String property) {
      return database.lookupByRID(rid, true).asDocument().get(property);
    }

    @Test
    void listOfDateTimePrecisionsAcceptsEveryDateValue() {
      int i = 0;
      for (final String declared : new String[] { "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS", "DATETIME_NANOS" }) {
        final String t = "L" + (i++);
        database.command("sql", "CREATE DOCUMENT TYPE " + t);
        database.command("sql", "CREATE PROPERTY " + t + ".p LIST OF " + declared);
        for (final Object v : new Object[] { LDT, LDT.toLocalDate(), Date.from(LDT.toInstant(ZoneOffset.UTC)),
            "2026-10-03 10:20:30.123456" }) {
          final RID[] rid = new RID[1];
          database.transaction(() -> rid[0] = database.newDocument(t).set("p", new ArrayList<>(List.of(v))).save().getIdentity());
          assertThat((List<?>) read(rid[0], "p")).as(declared + " " + v.getClass().getSimpleName()).hasSize(1);
        }
      }
    }

    @Test
    void listOfDateIsStoredAsDate() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.p LIST OF DATE");
      final RID[] rid = new RID[1];
      database.transaction(
          () -> rid[0] = database.newDocument("D").set("p", new ArrayList<>(List.of(LDT.toLocalDate(), "2026-10-04"))).save()
              .getIdentity());
      final List<?> stored = (List<?>) read(rid[0], "p");
      assertThat(stored).hasSize(2);
      assertThat(stored.get(0)).isEqualTo(LocalDate.of(2026, 10, 3));
      assertThat(stored.get(1)).isEqualTo(LocalDate.of(2026, 10, 4));
    }

    @Test
    void listOfDatetimeMicrosKeepsMicrosecondPrecision() {
      database.command("sql", "CREATE DOCUMENT TYPE M");
      database.command("sql", "CREATE PROPERTY M.p LIST OF DATETIME_MICROS");
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("M").set("p", new ArrayList<>(List.of(LDT))).save().getIdentity());
      assertThat(((List<?>) read(rid[0], "p")).get(0)).isEqualTo(LDT);
    }

    @Test
    void mapOfDateTypesAcceptsDateValues() {
      database.command("sql", "CREATE DOCUMENT TYPE MM");
      database.command("sql", "CREATE PROPERTY MM.a MAP OF DATETIME_MICROS");
      database.command("sql", "CREATE PROPERTY MM.b MAP OF DATE");
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("MM").set("a", new LinkedHashMap<>(Map.of("k", LDT)))
          .set("b", new LinkedHashMap<>(Map.of("k", "2026-10-03"))).save().getIdentity());
      assertThat(((Map<?, ?>) read(rid[0], "a")).get("k")).isEqualTo(LDT);
      assertThat(((Map<?, ?>) read(rid[0], "b")).get("k")).isEqualTo(LocalDate.of(2026, 10, 3));
    }

    @Test
    void sqlInsertIntoListOfDatetimeMicros() {
      database.command("sql", "CREATE DOCUMENT TYPE S");
      database.command("sql", "CREATE PROPERTY S.p LIST OF DATETIME_MICROS");
      database.transaction(() -> database.command("sql", "INSERT INTO S SET p = ['2026-10-03 10:20:30.123456']").close());
      assertThat(database.countType("S", true)).isEqualTo(1);
    }

    @Test
    void listOfDateStillRefusesNonDates() {
      database.command("sql", "CREATE DOCUMENT TYPE R");
      database.command("sql", "CREATE PROPERTY R.p LIST OF DATETIME_MICROS");
      assertThatThrownBy(() -> database.transaction(
          () -> database.newDocument("R").set("p", new ArrayList<>(List.of("not a date"))).save())).isInstanceOf(
          IllegalArgumentException.class).hasMessageContaining("'p'");
    }

    private void declareAddress() {
      database.command("sql", "CREATE DOCUMENT TYPE Address");
      database.command("sql", "CREATE DOCUMENT TYPE Other");
      database.command("sql", "CREATE DOCUMENT TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.addresses LIST OF Address");
    }

    @Test
    void listOfDocumentTypeRefusesScalars() {
      declareAddress();
      assertThatThrownBy(() -> database.transaction(
          () -> database.newDocument("Person").set("addresses", new ArrayList<>(List.of(1, "two"))).save())).isInstanceOf(
          ValidationException.class).hasMessageContaining("LIST of 'Address'");
      assertThatThrownBy(
          () -> database.transaction(() -> database.command("sql", "INSERT INTO Person SET addresses = [1, 'two']").close()))
          .isInstanceOf(ValidationException.class);
    }

    @Test
    void listOfDocumentTypeRefusesAMapOfAnotherType() {
      declareAddress();
      final Map<String, Object> other = new LinkedHashMap<>();
      other.put("@type", "Other");
      other.put("city", "Rome");
      assertThatThrownBy(() -> database.transaction(
          () -> database.newDocument("Person").set("addresses", new ArrayList<>(List.of(other))).save())).isInstanceOf(
          ValidationException.class).hasMessageContaining("Other");
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
          "INSERT INTO Person SET addresses = [{\"@type\": \"Other\", \"city\": \"Rome\"}]").close())).isInstanceOf(
          ValidationException.class);
    }

    @Test
    void listOfDocumentTypeTurnsAPlainMapIntoAnEmbeddedDocument() {
      declareAddress();
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Person")
          .set("addresses", new ArrayList<>(List.of(new LinkedHashMap<>(Map.of("city", "Rome"))))).save().getIdentity());
      final List<?> stored = (List<?>) read(rid[0], "addresses");
      assertThat(stored).hasSize(1);
      assertThat(stored.get(0)).isInstanceOf(EmbeddedDocument.class);
      assertThat(((EmbeddedDocument) stored.get(0)).getTypeName()).isEqualTo("Address");
      assertThat(((EmbeddedDocument) stored.get(0)).getString("city")).isEqualTo("Rome");
    }

    @Test
    void listOfDocumentTypeAcceptsATypedMapOfTheDeclaredType() {
      declareAddress();
      final Map<String, Object> address = new LinkedHashMap<>();
      address.put("@type", "Address");
      address.put("city", "Rome");
      database.transaction(() -> database.newDocument("Person").set("addresses", new ArrayList<>(List.of(address))).save());
      database.transaction(() -> database.command("sql", "INSERT INTO Person SET addresses = [{\"city\": \"Milan\"}]").close());
      assertThat(database.countType("Person", true)).isEqualTo(2);
    }

    @Test
    void mapOfDocumentTypeTurnsPlainMapValuesIntoEmbeddedDocuments() {
      database.command("sql", "CREATE DOCUMENT TYPE Address");
      database.command("sql", "CREATE DOCUMENT TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.byName MAP OF Address");
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Person")
          .set("byName", new LinkedHashMap<>(Map.of("home", new LinkedHashMap<>(Map.of("city", "Rome"))))).save().getIdentity());
      final Object home = ((Map<?, ?>) read(rid[0], "byName")).get("home");
      assertThat(home).isInstanceOf(EmbeddedDocument.class);
      assertThatThrownBy(() -> database.transaction(
          () -> database.newDocument("Person").set("byName", new LinkedHashMap<>(Map.of("home", 1))).save())).isInstanceOf(
          ValidationException.class);
    }

    @Test
    void listOfDocumentTypeAcceptsNullElement() {
      declareAddress();
      final List<Object> withNull = new ArrayList<>();
      withNull.add(null);
      database.transaction(() -> database.newDocument("Person").set("addresses", withNull).save());
      assertThat(database.countType("Person", true)).isEqualTo(1);
    }

    @Test
    void listOfStringStillRefusesADate() {
      database.command("sql", "CREATE DOCUMENT TYPE LS");
      database.command("sql", "CREATE PROPERTY LS.p LIST OF STRING");
      assertThatThrownBy(() -> database.transaction(
          () -> database.newDocument("LS").set("p", new ArrayList<>(List.of(new Date()))).save())).isInstanceOf(
          ValidationException.class);
    }

    @Test
    void listOfDateAcceptsInstantAndZonedDateTime() {
      database.command("sql", "CREATE DOCUMENT TYPE LZ");
      database.command("sql", "CREATE PROPERTY LZ.p LIST OF DATE");
      database.command("sql", "CREATE PROPERTY LZ.q LIST OF DATETIME_MICROS");
      database.transaction(() -> database.newDocument("LZ")
          .set("p", new ArrayList<>(List.of(Instant.now(), ZonedDateTime.now())))
          .set("q", new ArrayList<>(List.of(Instant.now(), ZonedDateTime.now()))).save());
      assertThat(database.countType("LZ", true)).isEqualTo(1);
    }

    @Test
    void listOfDocumentTypeStillAcceptsALinkWrittenAsAString() {
      declareAddress();
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Address").set("city", "Rome").save().getIdentity());
      database.transaction(() -> database.newDocument("Person").set("addresses", new ArrayList<>(List.of(rid[0].toString()))).save());
      assertThat(database.countType("Person", true)).isEqualTo(1);
    }

    @Test
    void jsonContentWithAPlainMapInAListOfDocumentType() {
      declareAddress();
      database.transaction(() -> database.command("sql", "INSERT INTO Person CONTENT {\"addresses\": [{\"city\": \"Rome\"}]}").close());
      final Object first = ((List<?>) database.query("sql", "SELECT FROM Person").next().getProperty("addresses")).get(0);
      assertThat(first).isInstanceOf(EmbeddedDocument.class);
      assertThat(((EmbeddedDocument) first).getTypeName()).isEqualTo("Address");
      assertThat(database.countType("Person", true)).isEqualTo(1);
    }

    @Test
    void mapOfDocumentTypeAcceptsNullValue() {
      database.command("sql", "CREATE DOCUMENT TYPE Address");
      database.command("sql", "CREATE DOCUMENT TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.byName MAP OF Address");
      final Map<String, Object> withNull = new LinkedHashMap<>();
      withNull.put("home", null);
      database.transaction(() -> database.newDocument("Person").set("byName", withNull).save());
      assertThat(database.countType("Person", true)).isEqualTo(1);
    }
  }
}
