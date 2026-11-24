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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12590: {@code MutableDocument.set(name, map)} looked the entry {@code "@type"} up in the map before it knew whether the map
 * was an embedded document, and a sorted map compares the String it is asked for with its own keys: a {@code TreeMap} of
 * {@code Character} or {@code Integer} keys failed the whole write with a {@link ClassCastException}. A map whose keys cannot be
 * Strings cannot carry an {@code "@type"} entry, so it is a plain map, exactly as the same entries in a {@link LinkedHashMap} are.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12590SortedMapNonStringKeysTest extends TestHelper {

  private Document writeAndReload(final String typeName, final String property, final Object value) {
    if (!database.getSchema().existsType(typeName))
      database.getSchema().createDocumentType(typeName);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set(property, value).save().getIdentity());
    return database.lookupByRID(rid[0], true).asDocument();
  }

  private static TreeMap<Character, Integer> letters() {
    final TreeMap<Character, Integer> letters = new TreeMap<>();
    letters.put('b', 2);
    letters.put('a', 1);
    return letters;
  }

  private static TreeMap<Integer, String> numbers() {
    final TreeMap<Integer, String> numbers = new TreeMap<>();
    numbers.put(2, "two");
    numbers.put(1, "one");
    return numbers;
  }

  @Test
  void aTreeMapOfCharacterKeysIsStored() {
    assertThat(writeAndReload("Letters", "m", letters()).get("m")).isEqualTo(Map.of("a", 1, "b", 2));
  }

  @Test
  void aTreeMapOfIntegerKeysIsStored() {
    assertThat(writeAndReload("Numbers", "m", numbers()).get("m")).isEqualTo(Map.of(1, "one", 2, "two"));
  }

  /**
   * The map decides what it can compare, not its class: a comparator over the same keys refuses the String just as natural ordering
   * does, and so does a concurrent sorted map.
   */
  @Test
  void aSortedMapOrderedByAComparatorIsStored() {
    final TreeMap<Character, Integer> descending = new TreeMap<>(Comparator.reverseOrder());
    descending.putAll(letters());
    assertThat(writeAndReload("Descending", "m", descending).get("m")).isEqualTo(Map.of("a", 1, "b", 2));

    final ConcurrentSkipListMap<Integer, String> concurrent = new ConcurrentSkipListMap<>(numbers());
    assertThat(writeAndReload("Concurrent", "m", concurrent).get("m")).isEqualTo(Map.of(1, "one", 2, "two"));
  }

  @Test
  void aDeclaredMapPropertyStoresASortedMapOfNonStringKeys() {
    database.getSchema().createDocumentType("DeclaredMap").createProperty("m", Type.MAP);
    assertThat(writeAndReload("DeclaredMap", "m", letters()).get("m")).isEqualTo(Map.of("a", 1, "b", 2));
    assertThat(writeAndReload("DeclaredMap", "m", numbers()).get("m")).isEqualTo(Map.of(1, "one", 2, "two"));
  }

  @Test
  void aMapOfDocumentTypeStoresASortedMapOfCharacterKeys() {
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.byInitial MAP OF Address");

    final TreeMap<Character, Map<String, Object>> byInitial = new TreeMap<>();
    byInitial.put('r', new LinkedHashMap<>(Map.of("city", "Rome")));
    byInitial.put('m', new LinkedHashMap<>(Map.of("city", "Milan")));

    final Map<?, ?> stored = (Map<?, ?>) writeAndReload("Person", "byInitial", byInitial).get("byInitial");
    assertThat(stored).hasSize(2);
    assertThat(((EmbeddedDocument) stored.get("m")).getString("city")).isEqualTo("Milan");
    assertThat(((EmbeddedDocument) stored.get("r")).getString("city")).isEqualTo("Rome");

    // THE KEY CLASS IS THE ONLY THING THAT MATTERS, NOT THE MAP'S ORDER: THE SAME ENTRIES IN A LinkedHashMap
    final Map<Character, Map<String, Object>> insertionOrder = new LinkedHashMap<>(byInitial);
    final Map<?, ?> storedLinked = (Map<?, ?>) writeAndReload("Person", "byInitial", insertionOrder).get("byInitial");
    assertThat(((EmbeddedDocument) storedLinked.get("m")).getString("city")).isEqualTo("Milan");
  }

  @Test
  void anEmbeddedDocumentTakesASortedMapOfNonStringKeys() {
    database.getSchema().createDocumentType("Inner");
    database.getSchema().createDocumentType("Outer");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument outer = database.newDocument("Outer");
      final MutableEmbeddedDocument inner = outer.newEmbeddedDocument("Inner", "e");
      inner.set("letters", letters());
      inner.set("numbers", numbers());
      rid[0] = outer.save().getIdentity();
    });

    final EmbeddedDocument inner = database.lookupByRID(rid[0], true).asDocument().getEmbedded("e");
    assertThat(inner.get("letters")).isEqualTo(Map.of("a", 1, "b", 2));
    assertThat(inner.get("numbers")).isEqualTo(Map.of(1, "one", 2, "two"));
  }

  @Test
  void aSortedMapOfNonStringKeysIsStoredBySql() {
    database.getSchema().createDocumentType("BySql");
    database.transaction(() -> database.command("sql", "INSERT INTO BySql SET m = ?", letters()).close());

    assertThat(database.query("sql", "SELECT FROM BySql").next().<Object>getProperty("m")).isEqualTo(Map.of("a", 1, "b", 2));
  }

  /**
   * The guard has to leave the real case alone: a sorted map of String keys that carries an {@code "@type"} is still an embedded
   * document, on a property the schema says nothing about and as an element of a {@code LIST OF} / {@code MAP OF} a document type.
   */
  @Test
  void aSortedMapOfStringKeysCarryingATypeIsStillAnEmbeddedDocument() {
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.addresses LIST OF Address");
    database.command("sql", "CREATE PROPERTY Person.byName MAP OF Address");

    final TreeMap<String, Object> typed = new TreeMap<>();
    typed.put("@type", "Address");
    typed.put("city", "Rome");

    final EmbeddedDocument schemaless = (EmbeddedDocument) writeAndReload("Anything", "e", typed).get("e");
    assertThat(schemaless.getTypeName()).isEqualTo("Address");
    assertThat(schemaless.getString("city")).isEqualTo("Rome");

    final TreeMap<String, Object> untyped = new TreeMap<>();
    untyped.put("city", "Milan");

    final Document person = writeAndReload("Person", "addresses", new ArrayList<>(List.of(typed, untyped)));
    final List<?> addresses = (List<?>) person.get("addresses");
    assertThat(addresses).hasSize(2);
    assertThat(((EmbeddedDocument) addresses.get(0)).getTypeName()).isEqualTo("Address");
    assertThat(((EmbeddedDocument) addresses.get(0)).getString("city")).isEqualTo("Rome");
    assertThat(((EmbeddedDocument) addresses.get(1)).getTypeName()).isEqualTo("Address");
    assertThat(((EmbeddedDocument) addresses.get(1)).getString("city")).isEqualTo("Milan");
  }
}
