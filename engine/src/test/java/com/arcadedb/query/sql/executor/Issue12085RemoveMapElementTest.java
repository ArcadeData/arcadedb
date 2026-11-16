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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12085: {@code UPDATE ... REMOVE list = <map>} removed the KEYS of the map from the list instead of the
 * map itself, so the documented {@code UPDATE Account REMOVE addresses = addresses[city = 'Rome']} removed nothing from a list
 * of maps or of embedded documents and worked only for a list of links. A map to remove is now one element of a list or a set,
 * equal by content, while {@code REMOVE mapProperty = 'key'} keeps removing a key.
 */
class Issue12085RemoveMapElementTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Account");
      database.command("sql", "CREATE DOCUMENT TYPE Address");
    });
  }

  @Test
  void listOfMapsRemoveByFilter() {
    insert("maps", "[{'city':'Rome'},{'city':'Paris'}]");
    assertThat(update("UPDATE Account REMOVE addresses = addresses[city = 'Rome'] WHERE name = 'maps'")).isEqualTo(1L);
    assertThat(cities("maps")).containsExactly("Paris");
  }

  @Test
  void listOfMapsRemoveByPosition() {
    insert("maps", "[{'city':'Rome'},{'city':'Paris'}]");
    update("UPDATE Account REMOVE addresses = addresses[0] WHERE name = 'maps'");
    assertThat(cities("maps")).containsExactly("Paris");
  }

  @Test
  void listOfMapsRemoveByEqualMapLiteral() {
    insert("maps", "[{'city':'Rome'},{'city':'Paris'}]");
    // a map that is not equal to an element, by one more entry, removes nothing
    update("UPDATE Account REMOVE addresses = {'city':'Rome','zip':1} WHERE name = 'maps'");
    assertThat(cities("maps")).containsExactly("Rome", "Paris");

    update("UPDATE Account REMOVE addresses = {'city':'Rome'} WHERE name = 'maps'");
    assertThat(cities("maps")).containsExactly("Paris");
  }

  @Test
  void equalMapRemovesFirstOccurrenceFilterRemovesEveryMatch() {
    insert("one", "[{'city':'Rome'},{'city':'Paris'},{'city':'Rome'}]");
    insert("all", "[{'city':'Rome'},{'city':'Paris'},{'city':'Rome'}]");

    // REMOVE x = <value> removes the first occurrence, as for a scalar
    update("UPDATE Account REMOVE addresses = {'city':'Rome'} WHERE name = 'one'");
    assertThat(cities("one")).containsExactly("Paris", "Rome");

    // the filter answers both Rome maps, and both are removed
    update("UPDATE Account REMOVE addresses = addresses[city = 'Rome'] WHERE name = 'all'");
    assertThat(cities("all")).containsExactly("Paris");
  }

  @Test
  void mapIsNotReadAsItsKeys() {
    insert("strings", "['city','Rome']");
    update("UPDATE Account REMOVE addresses = {'city':'Rome'} WHERE name = 'strings'");
    assertThat(addresses("strings")).containsExactly("city", "Rome");
  }

  @Test
  void listOfEmbeddedDocumentsRemoveByFilterAndPosition() {
    for (final String name : List.of("filter", "position"))
      database.transaction(() -> {
        final MutableDocument account = database.newDocument("Account").set("name", name).set("addresses", new ArrayList<>());
        account.newEmbeddedDocument("Address", "addresses").set("city", "Rome");
        account.newEmbeddedDocument("Address", "addresses").set("city", "Paris");
        account.save();
      });
    assertThat(addresses("filter")).hasSize(2).allMatch(EmbeddedDocument.class::isInstance);

    update("UPDATE Account REMOVE addresses = addresses[city = 'Rome'] WHERE name = 'filter'");
    update("UPDATE Account REMOVE addresses = addresses[0] WHERE name = 'position'");

    assertThat(cities("filter")).containsExactly("Paris");
    assertThat(cities("position")).containsExactly("Paris");
    assertThat(addresses("filter")).allMatch(EmbeddedDocument.class::isInstance);
  }

  /** The {@code {'@type':'Address', ...}} literal of the issue: SQL stores it in a list as a map that carries the type key. */
  @Test
  void listOfTypedMapLiteralsRemoveByFilter() {
    insert("typed", "[{'@type':'Address','city':'Rome'},{'@type':'Address','city':'Paris'}]");
    update("UPDATE Account REMOVE addresses = addresses[city = 'Rome'] WHERE name = 'typed'");
    assertThat(cities("typed")).containsExactly("Paris");
  }

  @Test
  void listOfLinksUnchanged() {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument("Address").set("city", "Rome").save().getIdentity();
      rids[1] = database.newDocument("Address").set("city", "Paris").save().getIdentity();
    });
    insert("links", "[" + rids[0] + "," + rids[1] + "]");

    update("UPDATE Account REMOVE addresses = addresses[city = 'Rome'] WHERE name = 'links'");
    assertThat(addresses("links")).containsExactly(rids[1]);
  }

  @Test
  void listOfStringsUnchanged() {
    insert("strings", "['Rome','Paris','Rome']");
    update("UPDATE Account REMOVE addresses = addresses[0] WHERE name = 'strings'");
    assertThat(addresses("strings")).containsExactly("Paris", "Rome");
  }

  @Test
  void mapPropertyStillRemovesKey() {
    database.transaction(() -> database.command("sql", "INSERT INTO Account SET name = 'map', m = {'k':1,'j':2}"));
    update("UPDATE Account REMOVE m = 'k' WHERE name = 'map'");
    reopenDatabase();
    final Map<String, Object> m = database.query("sql", "SELECT m FROM Account WHERE name = 'map'").next().getProperty("m");
    assertThat(m).containsOnlyKeys("j");
  }

  @Test
  void removeAndRemoveAllMethodsTakeAMapAsOneElement() {
    assertThat(value("SELECT [{'a':1},{'b':2},{'a':1}].remove({'a':1}) AS v")).isEqualTo(List.of(Map.of("b", 2), Map.of("a", 1)));
    assertThat(value("SELECT [{'a':1},{'b':2},{'a':1}].removeAll({'a':1}) AS v")).isEqualTo(List.of(Map.of("b", 2)));
    assertThat(value("SELECT ['a','b'].remove({'a':1}) AS v")).isEqualTo(List.of("a", "b"));
  }

  @Test
  void setOfMaps() {
    final Set<Object> set = new LinkedHashSet<>(List.of(Map.of("city", "Rome"), Map.of("city", "Paris")));
    MultiValue.remove(set, Map.of("city", "Rome"), false);
    assertThat(set).containsExactly(Map.of("city", "Paris"));

    final Set<Object> filtered = new LinkedHashSet<>(List.of(Map.of("city", "Rome"), Map.of("city", "Paris")));
    MultiValue.remove(filtered, List.of(Map.of("city", "Paris")), false);
    assertThat(filtered).containsExactly(Map.of("city", "Rome"));
  }

  private void insert(final String name, final String addresses) {
    database.transaction(() -> database.command("sql", "INSERT INTO Account SET name = ?, addresses = " + addresses, name));
  }

  private long update(final String sql) {
    final long[] count = new long[1];
    database.transaction(() -> count[0] = database.command("sql", sql).next().<Long>getProperty("count"));
    return count[0];
  }

  private Object value(final String sql) {
    return database.query("sql", sql).next().getProperty("v");
  }

  /** Reads the stored list from the record after a reopen, so a removal that was not persisted shows up. */
  private List<Object> addresses(final String name) {
    reopenDatabase();
    final List<Object> list = database.query("sql", "SELECT FROM Account WHERE name = ?", name).next().toElement()
        .getList("addresses");
    final List<Object> result = new ArrayList<>(list.size());
    for (final Object element : list)
      result.add(element instanceof Identifiable identifiable && !(element instanceof EmbeddedDocument) ?
          identifiable.getIdentity() :
          element);
    return result;
  }

  private List<String> cities(final String name) {
    final List<String> result = new ArrayList<>();
    for (final Object element : addresses(name))
      result.add(element instanceof Document document ? document.getString("city") : (String) ((Map<?, ?>) element).get("city"));
    return result;
  }
}
