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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UpdateStatementExecutionTest extends TestHelper {
  private String className;

  public UpdateStatementExecutionTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    final DocumentType clazz = database.getSchema().createDocumentType("OUpdateStatementExecutionTest");
    clazz.createProperty("name", Type.STRING);
    className = clazz.getName();

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("number", 4L);

      final List<String> tagsList = new ArrayList<>();
      tagsList.add("foo");
      tagsList.add("bar");
      tagsList.add("baz");
      doc.set("tagsList", tagsList);

      final Map<String, String> tagsMap = new HashMap<>();
      tagsMap.put("foo", "foo");
      tagsMap.put("bar", "bar");
      tagsMap.put("baz", "baz");
      doc.set("tagsMap", tagsMap);

      doc.save();
    }
  }

  @Override
  protected void endTest() {
    database.transaction(() ->
      database.command("sql", "delete from OUpdateStatementExecutionTest"));
    GlobalConfiguration.resetAll();
  }

  @Test
  void setString() {
    ResultSet result = database.command("sql", "update " + className + " set surname = 'foo'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 10L);

    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isEqualTo("foo");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void copyField() {
    ResultSet result = database.command("sql", "update " + className + " set surname = name");

    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo(10L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isEqualTo(item.getProperty("name"));
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void setExpression() {
    ResultSet result = database.command("sql", "update " + className + " set surname = 'foo'+name ");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo(10L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isEqualTo("foo" + item.getProperty("name"));
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void conditionalSet() {
    ResultSet result = database.command("sql", "update " + className + " set surname = 'foo' where name = 'name3'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo(1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      if ("name3".equals(item.getProperty("name"))) {
        assertThat(item.<String>getProperty("surname")).isEqualTo("foo");
        found = true;
      }
    }
    assertThat(found).isTrue();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void setOnList() {
    ResultSet result = database.command("sql", "update " + className + " set tagsList[0] = 'abc' where name = 'name3'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo(1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      if ("name3".equals(item.getProperty("name"))) {
        final List<String> tags = List.of("abc", "bar", "baz");
        assertThat(item.<List<String>>getProperty("tagsList")).isEqualTo(tags);
        found = true;
      }
    }
    assertThat(found).isTrue();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void setOnList2() {
    ResultSet result = database.command("sql", "update " + className + " set tagsList[6] = 'abc' where name = 'name3'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      if ("name3".equals(item.getProperty("name"))) {
        final List<String> tags = new ArrayList<>();
        tags.add("foo");
        tags.add("bar");
        tags.add("baz");
        tags.add(null);
        tags.add(null);
        tags.add(null);
        tags.add("abc");
        assertThat(item.<List<String>>getProperty("tagsList")).isEqualTo(tags);
        found = true;
      }
    }
    assertThat(found).isTrue();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void setOnMap() {
    ResultSet result = database.command("sql", "update " + className + " set tagsMap['foo'] = 'abc' where name = 'name3'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 1L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      if ("name3".equals(item.getProperty("name"))) {
        final Map<String, String> tags = new HashMap<>();
        tags.put("foo", "abc");
        tags.put("bar", "bar");
        tags.put("baz", "baz");
        assertThat(item.<Map<String, String>>getProperty("tagsMap")).isEqualTo(tags);
        found = true;
      } else {
        final Map<String, String> tags = new HashMap<>();
        tags.put("foo", "foo");
        tags.put("bar", "bar");
        tags.put("baz", "baz");
        assertThat(item.<Map<String, String>>getProperty("tagsMap")).isEqualTo(tags);
      }
    }
    assertThat(found).isTrue();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void plusAssignCollection() {
    ResultSet result = database.command("sql",
        "insert into " + className + " set listStrings = ['this', 'is', 'a', 'test'], listNumbers = [1,2,3]");
    final RID rid = result.next().getIdentity().get();
    result = database.command("sql", "update " + rid + " set listStrings += '!', listNumbers += 9");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();

    result = database.command("sql", "select from " + rid);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();

    List<String> listStrings = item.getProperty("listStrings");
    assertThat(listStrings.size()).isEqualTo(5);
    assertThat(listStrings.get(4)).isEqualTo("!");

    List<Number> listNumbers = item.getProperty("listNumbers");
    assertThat(listNumbers.size()).isEqualTo(4);
    assertThat(listNumbers.get(3)).isEqualTo(9);
  }

  @Test
  void plusAssignMap() {
    ResultSet result = database.command("sql", "insert into " + className + " set map1 = {'name':'Jay'}, map2 = {'name':'Jay'}");
    final RID rid = result.next().getIdentity().get();
    result = database.command("sql", "update " + rid + " set map1 += { 'last': 'Miner'}, map2 += [ 'last', 'Miner']");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();

    result = database.command("sql", "select from " + rid);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();

    Map<String, String> map1 = item.getProperty("map1");
    assertThat(map1.size()).isEqualTo(2);
    assertThat(map1.get("name")).isEqualTo("Jay");
    assertThat(map1.get("last")).isEqualTo("Miner");

    Map<String, String> map2 = item.getProperty("map2");
    assertThat(map2.size()).isEqualTo(2);
    assertThat(map2.get("name")).isEqualTo("Jay");
    assertThat(map2.get("last")).isEqualTo("Miner");
  }

  /**
   * Testcase for issue https://github.com/ArcadeData/arcadedb/issues/927
   */
  @Test
  void plusAssignNestedMaps() {
    ResultSet result = database.command("sql", "insert into " + className + " set map1 = {}");
    final RID rid = result.next().getIdentity().get();

    database.command("sql", "update " + rid + " set bars = map( \"23-03-24\" , { \"volume\": 100 } )");

    result = database.command("sql", "update " + rid + " set bars += { \"2023-03-08\": { \"volume\": 134 }}");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();

    result = database.command("sql", "select from " + rid);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();

    Map<String, Object> map1 = item.getProperty("bars");
    assertThat(map1.size()).isEqualTo(2);

    Map nestedMap = (Map) map1.get("23-03-24");
    assertThat(nestedMap).isNotNull();
    assertThat(nestedMap.get("volume")).isEqualTo(100);

    nestedMap = (Map) map1.get("2023-03-08");
    assertThat(nestedMap).isNotNull();
    assertThat(nestedMap.get("volume")).isEqualTo(134);
  }

  @Test
  void plusAssign() {
    ResultSet result = database.command("sql", "update " + className + " set name += 'foo', newField += 'bar', number += 5");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 10L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.getProperty("name").toString().endsWith("foo")).isTrue(); // test concatenate string to string
      assertThat(item.getProperty("name").toString().length()).isEqualTo(8);
      assertThat(item.<String>getProperty("newField")).isEqualTo("bar"); // test concatenate null to string
      assertThat(item.<Long>getProperty("number")).isEqualTo(9L); // test sum numbers
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void minusAssign() {
    ResultSet result = database.command("sql", "update " + className + " set number -= 5");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 10L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<Long>getProperty("number")).isEqualTo((Object) (-1L));
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void minusAssignCollection() {
    ResultSet result = database.command("sql",
        "insert into " + className + " set listStrings = ['this', 'is', 'a', 'test'], listNumbers = [1,2,3]");
    final RID rid = result.next().getIdentity().get();
    result = database.command("sql", "update " + rid + " set listStrings -= 'a', listNumbers -= 2");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();

    result = database.command("sql", "select from " + rid);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();

    List<String> listStrings = item.getProperty("listStrings");
    assertThat(listStrings.size()).isEqualTo(3);
    assertThat(listStrings.contains("!")).isFalse();

    List<Number> listNumbers = item.getProperty("listNumbers");
    assertThat(listNumbers.size()).isEqualTo(2);
    assertThat(listNumbers.contains(2)).isFalse();
  }

  @Test
  void minusAssignMap() {
    ResultSet result = database.command("sql", "insert into " + className + " set map1 = {'name':'Jay'}, map2 = {'name':'Jay'}");
    final RID rid = result.next().getIdentity().get();
    result = database.command("sql", "update " + rid + " set map1 -= {'name':'Jay'}, map2 -= [ 'name' ]");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();

    result = database.command("sql", "select from " + rid);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();

    Map<String, String> map1 = item.getProperty("map1");
    assertThat(map1.size()).isEqualTo(0);

    Map<String, String> map2 = item.getProperty("map2");
    assertThat(map2.size()).isEqualTo(0);
  }

  @Test
  void starAssign() {
    ResultSet result = database.command("sql", "update " + className + " set number *= 5");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 10L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<Long>getProperty("number")).isEqualTo(20L);
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void slashAssign() {
    ResultSet result = database.command("sql", "update " + className + " set number /= 2");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 10L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<Long>getProperty("number")).isEqualTo(2L);
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void remove() {
    ResultSet result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }

    result.close();
    result = database.command("sql", "update " + className + " remove surname");
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 10L);
    }
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.toElement().has("surname")).isFalse();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void content() {

    ResultSet result = database.command("sql", "update " + className + " content {'name': 'foo', 'secondName': 'bar'}");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 10L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("foo");
      assertThat(item.<String>getProperty("secondName")).isEqualTo("bar");
      assertThat(item.<String>getProperty("surname")).isNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void merge() {

    ResultSet result = database.command("sql", "update " + className + " merge {'name': 'foo', 'secondName': 'bar'}");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Long>getProperty("count")).isEqualTo((Object) 10L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("foo");
      assertThat(item.<String>getProperty("secondName")).isEqualTo("bar");
      assertThat(item.<String>getProperty("surname").startsWith("surname")).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void remove1() {
    final String className = "overridden" + this.className;

    final DocumentType clazz = database.getSchema().createDocumentType(className);
    clazz.createProperty("theProperty", Type.LIST);

    final MutableDocument doc = database.newDocument(className);
    final List theList = new ArrayList();
    for (int i = 0; i < 10; i++) {
      theList.add("n" + i);
    }
    doc.set("theProperty", theList);

    doc.save();

    ResultSet result = database.command("sql", "update " + className + " remove theProperty[0]");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    final List ls = item.getProperty("theProperty");
    assertThat(ls).isNotNull();
    assertThat(ls.size()).isEqualTo(9);
    assertThat(ls.contains("n0")).isFalse();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void remove2() {
    final String className = "overridden" + this.className;
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    clazz.createProperty("theProperty", Type.LIST);

    final MutableDocument doc = database.newDocument(className);
    final List theList = new ArrayList();
    for (int i = 0; i < 10; i++) {
      theList.add("n" + i);
    }
    doc.set("theProperty", theList);

    doc.save();

    ResultSet result = database.command("sql", "update " + className + " remove theProperty[0, 1, 3]");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    final List ls = item.getProperty("theProperty");

    assertThat(ls).isNotNull();
    assertThat(ls.size()).isEqualTo(7);
    assertThat(ls.contains("n0")).isFalse();
    assertThat(ls.contains("n1")).isFalse();
    assertThat(ls.contains("n2")).isTrue();
    assertThat(ls.contains("n3")).isFalse();
    assertThat(ls.contains("n4")).isTrue();

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void remove3() {
    final String className = "overridden" + this.className;
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    clazz.createProperty("theProperty", Type.EMBEDDED);

    final MutableDocument doc = database.newDocument(className);
    MutableEmbeddedDocument emb = doc.newEmbeddedDocument(className, "theProperty");
    emb.set("sub", "foo");
    emb.set("aaa", "bar");
    doc.set("theProperty", emb);

    doc.save();

    ResultSet result = database.command("sql", "update " + className + " remove theProperty.sub");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "SElect from " + className);
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    final EmbeddedDocument ls = item.getProperty("theProperty");
    assertThat(ls).isNotNull();
    assertThat(ls.getPropertyNames().contains("sub")).isFalse();
    assertThat(ls.getString("aaa")).isEqualTo("bar");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void removeFromMapSquare() {

    database.command("sql", "UPDATE " + className + " REMOVE tagsMap[\"bar\"]").close();

    final ResultSet result = database.query("sql", "SELECT tagsMap FROM " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(((Map) item.getProperty("tagsMap")).size()).isEqualTo(2);
      assertThat(((Map) item.getProperty("tagsMap")).containsKey("bar")).isFalse();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void removeFromMapEquals() {

    database.command("sql", "UPDATE " + className + " REMOVE tagsMap = \"bar\"").close();

    final ResultSet result = database.query("sql", "SELECT tagsMap FROM " + className);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(((Map) item.getProperty("tagsMap")).size()).isEqualTo(2);
      assertThat(((Map) item.getProperty("tagsMap")).containsKey("bar")).isFalse();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void returnBefore() {
    final ResultSet result = database.command("sql",
        "update " + className + " set name = 'foo' RETURN BEFORE where name = 'name1'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("name")).isEqualTo("name1");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void returnBeforeWithEmbeddedDocRemoveIsNotMutatedByTheRemoval() {
    // Issue #6517: UPDATE ... REMOVE <embeddedDoc>.<field> ... RETURN BEFORE must snapshot the embedded
    // document BEFORE the in-place field removal, not share the live reference with it.
    final String className = "overridden" + this.className;
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    clazz.createProperty("emb", Type.EMBEDDED);

    final MutableDocument doc = database.newDocument(className);
    final MutableEmbeddedDocument emb = doc.newEmbeddedDocument(className, "emb");
    emb.set("sub", "foo").set("aaa", "bar");
    doc.save();

    final ResultSet result = database.command("sql",
        "update " + className + " remove emb.sub RETURN BEFORE");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    final Map<String, Object> before = item.getProperty("emb");
    assertThat((String) before.get("sub")).isEqualTo("foo");
    assertThat((String) before.get("aaa")).isEqualTo("bar");
    assertThat(result.hasNext()).isFalse();
    result.close();

    final ResultSet persisted = database.query("sql", "SELECT emb FROM " + className);
    assertThat(persisted.hasNext()).isTrue();
    final EmbeddedDocument after = persisted.next().getProperty("emb");
    assertThat(after.getPropertyNames().contains("sub")).isFalse();
    assertThat(after.getString("aaa")).isEqualTo("bar");
    persisted.close();
  }

  @Test
  void returnBeforeWithListRemoveIsNotMutatedByTheRemoval() {
    // Issue #6456: UPDATE ... REMOVE <collection value> ... RETURN BEFORE must snapshot the collection
    // BEFORE the in-place removal, not share the live list reference with it.
    final ResultSet result = database.command("sql",
        "update " + className + " remove tagsList = 'bar' RETURN BEFORE where name = 'name1'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    final List<String> before = item.getProperty("tagsList");
    assertThat(before).containsExactly("foo", "bar", "baz");
    assertThat(result.hasNext()).isFalse();
    result.close();

    final ResultSet persisted = database.query("sql", "SELECT tagsList FROM " + className + " WHERE name = 'name1'");
    assertThat(persisted.hasNext()).isTrue();
    final List<String> after = persisted.next().getProperty("tagsList");
    assertThat(after).containsExactly("foo", "baz");
    persisted.close();
  }

  @Test
  void returnBeforeWithNestedListInsideMapRemoveIsNotMutatedByTheRemoval() {
    // Issue #6456 review follow-up: pin down the recursive branch of the deep copy, not just a top-level
    // List/Map, by mutating a List nested inside a Map value.
    final String className = "overridden" + this.className;
    database.getSchema().createDocumentType(className);

    final MutableDocument doc = database.newDocument(className);
    final Map<String, Object> nestedMap = new HashMap<>();
    final List<String> nestedList = new ArrayList<>();
    nestedList.add("foo");
    nestedList.add("bar");
    nestedList.add("baz");
    nestedMap.put("list", nestedList);
    doc.set("nestedMap", nestedMap);
    doc.save();

    final ResultSet result = database.command("sql",
        "update " + className + " remove nestedMap[\"list\"] = 'bar' RETURN BEFORE");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    final Map<String, Object> before = item.getProperty("nestedMap");
    assertThat((List<String>) before.get("list")).containsExactly("foo", "bar", "baz");
    assertThat(result.hasNext()).isFalse();
    result.close();

    final ResultSet persisted = database.query("sql", "SELECT nestedMap FROM " + className);
    assertThat(persisted.hasNext()).isTrue();
    final Map<String, Object> after = persisted.next().getProperty("nestedMap");
    assertThat((List<String>) after.get("list")).containsExactly("foo", "baz");
    persisted.close();
  }

  @Test
  void returnBeforeWithMapKeyRemoveIsNotMutatedByTheRemoval() {
    // Issue #6456: same shallow-copy bug for nested `REMOVE map[key]` on the map value.
    final ResultSet result = database.command("sql",
        "update " + className + " remove tagsMap[\"bar\"] RETURN BEFORE where name = 'name1'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    final Map<String, String> before = item.getProperty("tagsMap");
    assertThat(before).containsEntry("bar", "bar").hasSize(3);
    assertThat(result.hasNext()).isFalse();
    result.close();

    final ResultSet persisted = database.query("sql", "SELECT tagsMap FROM " + className + " WHERE name = 'name1'");
    assertThat(persisted.hasNext()).isTrue();
    final Map<String, String> after = persisted.next().getProperty("tagsMap");
    assertThat(after).doesNotContainKey("bar").hasSize(2);
    persisted.close();
  }

  @Test
  void localDateTimeUpsertWithIndexMicros() throws Exception {
    database.transaction(() -> {
      if (database.getSchema().existsType("Product"))
        database.getSchema().dropType("Product");

      DocumentType dtProduct = database.getSchema().createDocumentType("Product");
      dtProduct.createProperty("start", Type.DATETIME_MICROS);
      dtProduct.createProperty("stop", Type.DATETIME_MICROS);
      dtProduct.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "start", "stop");
    });

    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(LocalDateTime.class);

    DateTimeFormatter FILENAME_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");

    database.transaction(() -> {
      LocalDateTime start = LocalDateTime.parse("20220318T215523", FILENAME_TIME_FORMAT);
      LocalDateTime stop = LocalDateTime.parse("20221129T002322", FILENAME_TIME_FORMAT);
      database.command("sql", "INSERT INTO Product SET start = ?, stop = ?", start, stop);
    });

    database.transaction(() -> {
      LocalDateTime start = LocalDateTime.parse("20220318T215523", FILENAME_TIME_FORMAT);
      LocalDateTime stop = LocalDateTime.parse("20220320T002321", FILENAME_TIME_FORMAT);
      database.command("sql", "INSERT INTO Product SET start = ?, stop = ?", start, stop);
    });

    database.transaction(() -> {
      Result result;

      /** ENTRIES:
       2022-03-18T21:55:23 - 2022-11-29T00:23:22
       2022-03-18T21:55:23 - 2022-03-20T00:23:21
       *     */
      LocalDateTime start = LocalDateTime.parse("2022-03-19T00:26:24.404379",
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
      LocalDateTime stop = LocalDateTime.parse("2022-03-19T00:28:26.525650",
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
      try (ResultSet resultSet = database.query("sql",
          "SELECT start, stop FROM Product WHERE start <= ? AND stop >= ? ORDER BY start DESC, stop DESC LIMIT 1", start, stop)) {

        assertThat(resultSet.hasNext()).isTrue();

        while (resultSet.hasNext()) {
          result = resultSet.next();
          assertThat(result.<LocalDateTime>getProperty("start")).isNotNull();
          assertThat(result.<LocalDateTime>getProperty("stop")).isNotNull();
        }
      }
    });
  }

  @Test
  void compositeIndexLookup() {
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    GlobalConfiguration.TX_RETRIES.setValue(0);
    final TypeIndex[] typeIndex = new TypeIndex[1];

    database.rollbackAllNested();

    database.transaction(() -> {
      DocumentType dtOrders = database.getSchema().createDocumentType("Order");
      dtOrders.createProperty("id", Type.STRING);
      dtOrders.createProperty("processor", Type.STRING);
      dtOrders.createProperty("status", Type.STRING);
      typeIndex[0] = dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
    });

    String processor = "SIR1LRM-7.1";
    for (int i = 0; i < 2; i++) {
      int id = i + 1;
      database.transaction(() -> {
        String sqlString = "INSERT INTO Order SET id = ?, status = ?, processor = ?";
        try (ResultSet resultSet1 = database.command("sql", sqlString, id, "PENDING", processor)) {
        }
      });
    }
    // update first record
    database.transaction(() -> {
      String sqlString = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
      try (ResultSet resultSet1 = database.command("sql", sqlString, "ERROR", 1)) {
      }
    });
    // select records with status = 'PENDING'
    database.transaction(() -> {
      String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
      try (ResultSet resultSet1 = database.query("sql", sqlString, "PENDING")) {
        assertThat(resultSet1.hasNext()).isTrue();
        final Result record = resultSet1.next();
        assertThat(record.<String>getProperty("status")).isEqualTo("PENDING");
        assertThat(record.<String>getProperty("id")).isEqualTo("2");
      }
    });
    // drop index
    database.getSchema().dropIndex(typeIndex[0].getName());

    // repeat select records with status = 'PENDING'
    database.transaction(() -> {
      String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
      try (ResultSet resultSet1 = database.query("sql", sqlString, "PENDING")) {
        assertThat(resultSet1.next().<String>getProperty("status")).isEqualTo("PENDING");
      }
    });

    database.transaction(() -> {
      try (ResultSet resultSet1 = database.query("sql",
          "SELECT id, status FROM Order WHERE status = 'PENDING' OR status = 'READY' ORDER BY id ASC LIMIT 1")) {
        assertThat(resultSet1.next().<String>getProperty("status")).isEqualTo("PENDING");
      }
    });
  }

  @Test
  void selectAfterUpdate() {
    database.transaction(() -> {
      DocumentType dtOrders = database.getSchema().buildDocumentType().withName("Order").create();
      dtOrders.createProperty("id", Type.INTEGER);
      dtOrders.createProperty("processor", Type.STRING);
      dtOrders.createProperty("status", Type.STRING);
      dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
      dtOrders.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
    });

    String INSERT_ORDER = "INSERT INTO Order SET id = ?, status = ?";
    database.begin();
    try (ResultSet resultSet = database.command("sql", INSERT_ORDER, 1, "PENDING")) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    database.commit();
    // update order
    database.begin();
    String UPDATE_ORDER = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
    try (ResultSet resultSet = database.command("sql", UPDATE_ORDER, "ERROR", 1)) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    database.commit();
    // resubmit order 1
    database.begin();
    // "UPDATE Order SET status = ? WHERE (status != ? AND status != ?) AND id = ?";
    String RESUBMIT_ORDER = "UPDATE Order SET status = ? RETURN AFTER WHERE (status != ? AND status != ?) AND id = ?";
    try (ResultSet resultSet = database.command("sql", RESUBMIT_ORDER, "PENDING", "PENDING", "PROCESSING", 1)) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    database.commit();
    // retrieve order 1 by id
    try (ResultSet resultSet = database.query("sql", "SELECT FROM Order WHERE id = 1")) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    // retrieve order 1 by status
    String RETRIEVE_NEXT_ELIGIBLE_ORDERS = "SELECT id, status FROM Order WHERE status = 'PENDING' OR status = 'READY' ORDER BY id ASC LIMIT 1";
    try (ResultSet resultSet = database.query("sql", RETRIEVE_NEXT_ELIGIBLE_ORDERS)) {
      assertThat(resultSet.hasNext()).isTrue();
    }
  }

  //@Test
  // STILL NOT SUPPORTED
  // ISSUE REPORTED ON DISCORD CHANNEL
  public void testUpdateVariable() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("Account")) {
        database.getSchema().createVertexType("Account");
      }
    });

    for (int i = 0; i < 10; i++) {
      database.transaction(() -> {
        database.command("sqlscript", "let account = create vertex Account set name = 'Luke';\n" +//
            "let e = Update [ #9:9 ] set name = 'bob';\n" +//
            "commit retry 100;\n" +//
            "return $e;");

        ResultSet resultSet = database.query("sql", "SELECT from Account where name = 'bob'");
        assertThat(resultSet.hasNext()).isTrue();
        while (resultSet.hasNext())
          assertThat(resultSet.next().<String>getProperty("name")).isEqualTo("bob");
      });
    }
  }

  // Issue #3813: UPDATE resolves an INSERT subquery returning @rid as the update target
  @Test
  void updateSubqueryInsertReturnRid() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");

    final ResultSet result = database.command("sql", "UPDATE (INSERT INTO Doc RETURN @rid) SET b = 3");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  // Issue #3813: UPDATE resolves an INSERT subquery returning the full element as the update target
  @Test
  void updateSubqueryInsertNoReturn() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");

    final ResultSet result = database.command("sql", "UPDATE (INSERT INTO Doc) SET b = 3");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  // Issue #3813: SQLScript LET + UPDATE Doc SET ... WHERE @rid = $x.@rid[0] resolves the LET variable
  @Test
  void sqlScriptLetUpdateWhereRid() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");

    final ResultSet result = database.command("sqlscript",
        """
            LET $x = INSERT INTO Doc RETURN @rid;
            UPDATE Doc SET b = 3 WHERE @rid = $x.@rid[0];\
            """);
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  // Issue #3813: SQLScript LET + UPDATE $x.@rid[0] SET ... uses the LET variable directly as the UPDATE target
  @Test
  void sqlScriptLetUpdateTarget() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");

    final ResultSet result = database.command("sqlscript",
        """
            LET $x = INSERT INTO Doc RETURN @rid;
            UPDATE $x.@rid[0] SET b = 3;\
            """);
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  // Issue #3813: UPDATE WHERE @rid = (INSERT INTO Doc RETURN @rid).@rid[0] resolves the INSERT subquery to a RID for comparison
  @Test
  void updateWhereRidEqualsSubqueryExpression() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");

    final ResultSet result = database.command("sql",
        "UPDATE Doc SET b = 3 WHERE @rid = (INSERT INTO Doc RETURN @rid).@rid[0]");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  // Issue #1814: UPDATE CONTENT without the mandatory property fails when APPLY DEFAULTS is not used
  @Test
  void updateContentWithoutMandatoryPropertyFails() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.prop STRING (mandatory true, notnull true, default 'Hi')");
      database.command("sql", "CREATE PROPERTY doc.other STRING");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc CONTENT { \"prop\": \"Ho\", \"other\": \"value\" }");

      ResultSet result = database.query("sql", "SELECT FROM doc WHERE prop = 'Ho'");
      assertThat(result.hasNext()).isTrue();
    });

    assertThatThrownBy(() ->
      database.transaction(() -> {
        database.command("sql", "UPDATE doc CONTENT { \"other\": \"new\" } WHERE prop = 'Ho'");
      })).isInstanceOf(ValidationException.class)
        .hasMessageContaining("mandatory");
  }

  // Issue #1814: UPDATE CONTENT with APPLY DEFAULTS triggers default value for missing mandatory property
  @Test
  void updateContentWithApplyDefaultsSucceeds() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.prop STRING (mandatory true, notnull true, default 'Hi')");
      database.command("sql", "CREATE PROPERTY doc.other STRING");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc CONTENT { \"prop\": \"Ho\", \"other\": \"value\" }");

      ResultSet result = database.query("sql", "SELECT FROM doc WHERE prop = 'Ho'");
      assertThat(result.hasNext()).isTrue();
    });

    database.transaction(() -> {
      database.command("sql", "UPDATE doc CONTENT { \"other\": \"new\" } APPLY DEFAULTS WHERE prop = 'Ho'");

      ResultSet result = database.query("sql", "SELECT FROM doc WHERE other = 'new'");
      assertThat(result.hasNext()).isTrue();
      var record = result.next();
      assertThat((String) record.getProperty("prop")).isEqualTo("Hi");
      assertThat((String) record.getProperty("other")).isEqualTo("new");
    });
  }

  // Issue #1814: UPDATE SET prop = null APPLY DEFAULTS resets the property to its schema default value
  @Test
  void updateSetWithApplyDefaultsSucceeds() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.prop STRING (mandatory true, notnull true, default 'Hi')");
      database.command("sql", "CREATE PROPERTY doc.other STRING");
    });

    database.transaction(() ->
      database.command("sql", "INSERT INTO doc CONTENT { \"prop\": \"Ho\", \"other\": \"value\" }"));

    database.transaction(() -> {
      database.command("sql", "UPDATE doc SET prop = null, other = 'changed' APPLY DEFAULTS WHERE prop = 'Ho'");

      ResultSet result = database.query("sql", "SELECT FROM doc WHERE other = 'changed'");
      assertThat(result.hasNext()).isTrue();
      var record = result.next();
      assertThat((String) record.getProperty("prop")).isEqualTo("Hi");
    });
  }

  // Issue #1814: UPDATE without APPLY DEFAULTS does NOT trigger default values - preserves existing property value
  @Test
  void updateWithoutApplyDefaultsPreservesOriginalBehavior() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.prop STRING (mandatory true, notnull true, default 'Hi')");
      database.command("sql", "CREATE PROPERTY doc.other STRING");
    });

    database.transaction(() ->
      database.command("sql", "INSERT INTO doc CONTENT { \"prop\": \"Original\", \"other\": \"value\" }"));

    database.transaction(() -> {
      database.command("sql", "UPDATE doc SET other = 'changed' WHERE prop = 'Original'");

      ResultSet result = database.query("sql", "SELECT FROM doc WHERE other = 'changed'");
      assertThat(result.hasNext()).isTrue();
      var record = result.next();
      assertThat((String) record.getProperty("prop")).isEqualTo("Original");
    });
  }

  /**
   * Issue #1814: APPLY DEFAULTS evaluates dynamic default expressions (e.g. sysdate) on UPDATE.
   * <p>
   * The default has to be an EXPRESSION. Written as {@code default "sysdate(...)"} - a double-quoted SQL string
   * literal - the call never happened and the literal text was stored verbatim, so the test asserted only that a
   * constant string is not null and could not fail if dynamic-default evaluation regressed (issue #6414, item 3).
   * The assertions below are on the value itself: it has to parse as a date, and after APPLY DEFAULTS it has to have
   * replaced a sentinel the expression cannot produce.
   */
  @Test
  void updateWithApplyDefaultsAndDynamicDefault() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE timestamped");
      database.command("sql", "CREATE PROPERTY timestamped.name STRING");
      database.command("sql", "CREATE PROPERTY timestamped.updated STRING (default sysdate().format('yyyy-MM-dd'))");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO timestamped SET name = 'test'");
      assertThatIsADate(readUpdated());
    });

    database.transaction(() -> {
      // A sentinel the default cannot produce: after APPLY DEFAULTS re-evaluates the expression it must be gone.
      database.command("sql", "UPDATE timestamped SET updated = 'stale' WHERE name = 'test'");
      database.command("sql", "UPDATE timestamped SET updated = null APPLY DEFAULTS WHERE name = 'test'");
      assertThatIsADate(readUpdated());
    });
  }

  private String readUpdated() {
    try (final ResultSet result = database.query("sql", "SELECT FROM timestamped WHERE name = 'test'")) {
      assertThat(result.hasNext()).isTrue();
      return result.next().getProperty("updated");
    }
  }

  /**
   * Deliberately not an equality against today's date: the two transactions above can straddle midnight. Parsing is
   * enough to prove the expression ran, because the stored literal it replaces - and the 'stale' sentinel - do not.
   */
  private static void assertThatIsADate(final String value) {
    assertThat(value).matches("\\d{4}-\\d{2}-\\d{2}");
    assertThat(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"))).isNotNull();
  }

  /**
   * The tests moved here from classes that ran with no transaction open: this class starts one before every test
   * ({@code autoStartTx}), so they close it first to run on the state they were written for.
   */
  private void commitAutoStartedTransaction() {
    if (database.isTransactionActive())
      database.commit();
  }

  // ---- #8027: nested SET persists ----
  //
  // Regression test for issue #8027.
  // <p>
  // {@code UPDATE ... SET <prop>.<key> = v} and {@code UPDATE ... SET <prop>[<index>] = v} reach into the container
  // the record already holds and mutate it IN PLACE, never going through {@code MutableDocument.set()} - the only
  // thing that sets the record's dirty flag. {@code SaveElementStep} then skips the save of a record it believes
  // clean (a skip that exists to avoid a needless MVCC version bump), so the write was lost. Three things made it
  // silent: the statement reported {@code count: 1}, {@code RETURN AFTER} reported the CHANGED value because it
  // reads back the mutated in-memory row, and both whole-property SET and nested SET on an embedded document
  // worked.
  // <p>
  // The REMOVE side of exactly this was fixed for issue #4730 and has carried a {@code markOwnerDirty} call since;
  // the SET side never got one. Every assertion below re-reads the value in a LATER transaction, which is the only
  // place the loss was visible.

  private static final String NESTED_SET_TYPE = "Issue8027Doc";

  private void createNestedSetTypes() {
    commitAutoStartedTransaction();
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + NESTED_SET_TYPE);
      database.command("sql", "CREATE DOCUMENT TYPE Issue8027Emb");
    });
  }

  /** Regression test for issue #8027. */
  @Test
  void aNestedMapKeySetWithDotNotationSurvivesTheCommit() {
    createNestedSetTypes();
    insert();
    updateOneRow("UPDATE " + NESTED_SET_TYPE + " SET m.a = 9");
    assertThat(map()).containsEntry("a", 9).containsEntry("b", 2);
  }

  /** Regression test for issue #8027. */
  @Test
  void aNestedMapKeySetWithBracketNotationSurvivesTheCommit() {
    createNestedSetTypes();
    insert();
    updateOneRow("UPDATE " + NESTED_SET_TYPE + " SET m['a'] = 8");
    assertThat(map()).containsEntry("a", 8).containsEntry("b", 2);
  }

  /**
   * Regression test for issue #8027.
   * <p>
   * A key the map did not carry: the in-place put ADDS it, and that addition was lost the same way.
   */
  @Test
  void aNewNestedMapKeySurvivesTheCommit() {
    createNestedSetTypes();
    insert();
    updateOneRow("UPDATE " + NESTED_SET_TYPE + " SET m.c = 3");
    assertThat(map()).containsEntry("a", 1).containsEntry("b", 2).containsEntry("c", 3);
  }

  /** Regression test for issue #8027. */
  @Test
  void aListElementSetByIndexSurvivesTheCommit() {
    createNestedSetTypes();
    insert();
    updateOneRow("UPDATE " + NESTED_SET_TYPE + " SET l[0] = 'Q'");
    assertThat(list()).containsExactly("Q", "y", "z");
  }

  /** Regression test for issue #8027. */
  @Test
  void aListRangeSetSurvivesTheCommit() {
    createNestedSetTypes();
    insert();
    updateOneRow("UPDATE " + NESTED_SET_TYPE + " SET l[0..2] = 'R'");
    assertThat(list()).containsExactly("R", "R", "z");
  }

  /**
   * Regression test for issue #8027.
   * <p>
   * {@code RETURN AFTER} is what an application uses to verify the write, and it reported the change even when the
   * change never reached disk. It must keep reporting it - and now agree with what a later transaction reads.
   */
  @Test
  void returnAfterAgreesWithWhatTheNextTransactionReads() {
    createNestedSetTypes();
    insert();
    final Object[] returned = new Object[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE " + NESTED_SET_TYPE + " SET m.a = 9 RETURN AFTER")) {
        returned[0] = rs.next().<Map<String, Object>>getProperty("m").get("a");
      }
    });
    assertThat(returned[0]).isEqualTo(9);
    assertThat(map()).containsEntry("a", 9);
  }

  /**
   * Regression test for issue #8027.
   * <p>
   * The two shapes that always worked, kept as the control the issue measures the broken ones against.
   */
  @Test
  void theShapesThatAlreadyWorkedStillWork() {
    createNestedSetTypes();
    insert();

    updateOneRow("UPDATE " + NESTED_SET_TYPE + " SET m = {'a': 7, 'b': 2}");
    assertThat(map()).containsEntry("a", 7);

    updateOneRow("UPDATE " + NESTED_SET_TYPE + " SET emb.x = 7");
    assertThat(embeddedX()).isEqualTo(7);

    updateOneRow("UPDATE " + NESTED_SET_TYPE + " REMOVE m.b");
    assertThat(map()).doesNotContainKey("b");
  }

  private void insert() {
    database.transaction(() -> database.command("sql", "INSERT INTO " + NESTED_SET_TYPE
        + " SET k = 1, m = {'a':1,'b':2}, l = ['x','y','z'], emb = {'@type':'Issue8027Emb','x':1,'y':2}"));
  }

  private void updateOneRow(final String sql) {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql)) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });
  }

  private Map<String, Object> map() {
    return read("m");
  }

  private List<String> list() {
    return read("l");
  }

  private Object embeddedX() {
    final Object embedded = read("emb");
    return embedded instanceof Document doc ? doc.get("x") : ((Map<?, ?>) embedded).get("x");
  }

  private <T> T read(final String property) {
    final Object[] holder = new Object[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + NESTED_SET_TYPE)) {
        holder[0] = rs.next().getProperty(property);
      }
    });
    return (T) holder[0];
  }

  // ---- #8032: REMOVE prop[n] on a Set ----
  //
  // Regression test for issue #8032: {@code ArraySingleValuesSelector.applyRemove()}'s {@code Set} branch declared its
  // running position as {@code final int count = 0} and never incremented it, so {@code values.contains(count)} tested
  // the same constant on every iteration. {@code REMOVE prop[0]} therefore matched every element and emptied the whole
  // set, while {@code REMOVE prop[n]} for any other {@code n} matched nothing and silently removed no element.

  private void createRemovePositionalType() {
    database.getSchema().createDocumentType("Issue8032Type");
  }

  /** Regression test for issue #8032. */
  @Test
  void removeFirstPositionOnSetRemovesOnlyThatElement() {
    createRemovePositionalType();
    final MutableDocument doc = database.newDocument("Issue8032Type");
    final Set<String> tags = new LinkedHashSet<>(List.of("a", "b", "c"));
    doc.set("tags", tags);
    doc.save();

    database.command("sql", "UPDATE Issue8032Type REMOVE tags[0] WHERE @rid = ?", doc.getIdentity());

    // the same Set instance the document was saved with must have exactly 2 elements left, not 0 (in-tx exposure)
    assertThat(tags).containsExactly("b", "c");

    // re-read in a fresh transaction to confirm what was actually persisted, not just kept in memory
    database.commit();
    database.begin();
    final ResultSet rs = database.query("sql", "SELECT tags FROM Issue8032Type");
    final List<Object> persisted = rs.next().getProperty("tags");
    assertThat(persisted).containsExactly("b", "c");
  }

  /** Regression test for issue #8032. */
  @Test
  void removeSecondPositionOnSetRemovesOnlyThatElement() {
    createRemovePositionalType();
    final MutableDocument doc = database.newDocument("Issue8032Type");
    final Set<String> tags = new LinkedHashSet<>(List.of("a", "b", "c"));
    doc.set("tags", tags);
    doc.save();

    database.command("sql", "UPDATE Issue8032Type REMOVE tags[1] WHERE @rid = ?", doc.getIdentity());

    assertThat(tags).containsExactly("a", "c");

    database.commit();
    database.begin();
    final ResultSet rs = database.query("sql", "SELECT tags FROM Issue8032Type");
    final List<Object> persisted = rs.next().getProperty("tags");
    assertThat(persisted).containsExactly("a", "c");
  }

  // ---- #8164: SET prop[n] on a Set ----
  //
  // Regression test for issue #8164: {@code ArraySelector.setValue(Set, int, ...)} rebuilt the set into a local
  // {@code result} and wrote it back with {@code target.clear(); target.addAll(result);} from INSIDE the rebuild loop,
  // while the iterator over {@code target} was still live. The second {@code next()} then died with a
  // {@code java.util.ConcurrentModificationException}, so {@code UPDATE ... SET prop[n] = value} ALWAYS failed on a
  // {@code Set}-valued property, leaving the in-flight value truncated to one element.
  // <p>
  // The positional REMOVE on the same value was fixed by #8112 (issue #8032); this is the write half of the same
  // family. The {@code List} branch next door is the control: it has always answered correctly, and is what the same
  // statement gets once the value has been through a save and reload.

  private static final String SET_POSITIONAL_TYPE = "Issue8164Type";

  private void createSetPositionalType() {
    database.getSchema().createDocumentType(SET_POSITIONAL_TYPE);
  }

  /** Regression test for issue #8164. */
  @Test
  void setFirstPositionOnSetReplacesOnlyThatElement() {
    createSetPositionalType();
    assertSetValueAt(0, List.of("Z", "b", "c"));
  }

  /** Regression test for issue #8164. */
  @Test
  void setInteriorPositionOnSetReplacesOnlyThatElement() {
    createSetPositionalType();
    assertSetValueAt(1, List.of("a", "Z", "c"));
  }

  /** Regression test for issue #8164. */
  @Test
  void setLastPositionOnSetReplacesOnlyThatElement() {
    createSetPositionalType();
    assertSetValueAt(2, List.of("a", "b", "Z"));
  }

  /**
   * Regression test for issue #8164.
   * <p>
   * An index past the end pads with {@code null} up to it, as the {@code List} branch does - except that a
   * {@code Set} refuses the duplicate, so several gaps collapse into ONE {@code null} and the value lands right
   * after it. The issue's expected {@code [a, b, c, null, null, Z]} is not representable in a set at all; this
   * pins what is, so the divergence from the {@code List} branch is a stated property rather than a surprise.
   */
  @Test
  void setPositionPastTheEndPadsWithASingleNullBecauseASetCollapsesTheGaps() {
    createSetPositionalType();
    assertSetValueAt(5, Arrays.asList("a", "b", "c", null, "Z"));
    // one gap, where the collapse cannot show, IS exactly what the List branch gives
    assertSetValueAt(3, Arrays.asList("a", "b", "c", "Z"));
  }

  /**
   * Regression test for issue #8164.
   * <p>
   * The {@code List} control, the shape the same property takes after a save and reload. It answered correctly
   * before the fix and must keep doing so.
   */
  @Test
  void theListBranchAnswersTheSameWayItAlwaysHas() {
    createSetPositionalType();
    for (final int idx : new int[] { 0, 1, 2 }) {
      final MutableDocument doc = database.newDocument(SET_POSITIONAL_TYPE);
      doc.set("tags", new ArrayList<>(List.of("a", "b", "c")));
      doc.save();

      database.command("sql", "UPDATE " + SET_POSITIONAL_TYPE + " SET tags[" + idx + "]='Z' WHERE @rid = ?", doc.getIdentity());

      final List<String> expected = new ArrayList<>(List.of("a", "b", "c"));
      expected.set(idx, "Z");
      assertThat(readPersistedTags(doc)).containsExactlyElementsOf(expected);
    }
  }

  /**
   * Regression test for issue #8164.
   * <p>
   * A positional SET and the positional REMOVE #8112 fixed must agree on the same value: the two halves of the
   * same statement family stopped disagreeing with this fix.
   */
  @Test
  void setAndRemoveAgreeOnTheSameSetValue() {
    createSetPositionalType();
    final MutableDocument doc = database.newDocument(SET_POSITIONAL_TYPE);
    doc.set("tags", new LinkedHashSet<>(List.of("a", "b", "c")));
    doc.save();

    database.command("sql", "UPDATE " + SET_POSITIONAL_TYPE + " SET tags[1]='Z' WHERE @rid = ?", doc.getIdentity());
    database.command("sql", "UPDATE " + SET_POSITIONAL_TYPE + " REMOVE tags[0] WHERE @rid = ?", doc.getIdentity());

    assertThat(readPersistedTags(doc)).containsExactly("Z", "c");
  }

  private void assertSetValueAt(final int idx, final List<String> expected) {
    final MutableDocument doc = database.newDocument(SET_POSITIONAL_TYPE);
    final Set<String> tags = new LinkedHashSet<>(List.of("a", "b", "c"));
    doc.set("tags", tags);
    doc.save();

    database.command("sql", "UPDATE " + SET_POSITIONAL_TYPE + " SET tags[" + idx + "]='Z' WHERE @rid = ?", doc.getIdentity());

    // the very Set instance the document was saved with: the defect left it truncated to a single element
    assertThat(tags).containsExactlyElementsOf(expected);

    assertThat(readPersistedTags(doc)).containsExactlyElementsOf(expected);
  }

  /** Re-reads in a fresh transaction, to confirm what was actually stored rather than only what is in memory. */
  private List<String> readPersistedTags(final MutableDocument doc) {
    database.commit();
    database.begin();
    try (final ResultSet rs = database.query("sql", "SELECT tags FROM " + SET_POSITIONAL_TYPE + " WHERE @rid = ?", doc.getIdentity())) {
      return rs.next().getProperty("tags");
    }
  }

  // ---- #8814: Halloween problem on an indexed range UPDATE ----
  //
  // #8814 (Halloween problem): an UPDATE that moves an indexed property forward, with a WHERE range on that same
  // property, walked the index range it was changing and met its own new keys ahead of the cursor, updating the same
  // records again and again.

  private static final int N = 2000;

  private void load() {
    database.transaction(() -> {
      final var type = database.getSchema().createVertexType("V");
      type.createProperty("a", Type.LONG);
      type.createProperty("b", Type.LONG);
      database.getSchema().buildTypeIndex("V", new String[] { "a" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < N; i++)
        database.newVertex("V").set("a", (long) i, "b", 0L).save();
    });
  }

  private void assertEachMovedOnce(final String where, final long expectedUpdated) {
    database.begin();
    final long returned;
    try (final ResultSet rs = database.command("sql", "UPDATE V SET a = a + 10, b = b + 1 WHERE " + where)) {
      returned = rs.next().<Number>getProperty("count").longValue();
    }
    database.commit();
    assertThat(returned).isEqualTo(expectedUpdated);

    long once = 0;
    long more = 0;
    try (final ResultSet rs = database.query("sql", "SELECT b FROM V")) {
      while (rs.hasNext()) {
        final long b = rs.next().<Number>getProperty("b").longValue();
        if (b == 1)
          once++;
        else if (b > 1)
          more++;
      }
    }
    assertThat(more).isZero();
    assertThat(once).isEqualTo(expectedUpdated);
  }

  /** Regression test for issue #8814. */
  @ParameterizedTest
  @ValueSource(strings = { "a < 1001", "a >= 0 AND a <= 1000", "a BETWEEN 0 AND 1000" })
  void rangeSpellings(final String where) {
    commitAutoStartedTransaction();
    load();
    assertEachMovedOnce(where, 1001);
  }

  /** Regression test for issue #8814. */
  @Test
  void limitStillStopsEarly() {
    commitAutoStartedTransaction();
    final String where = "a >= 0 AND a <= 1000";
    load();
    database.begin();
    try (final ResultSet rs = database.command("sql", "UPDATE V SET a = a + 10, b = b + 1 WHERE " + where + " LIMIT 5")) {
      assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(5);
    }
    database.commit();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V WHERE b > 0")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(5);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V WHERE b = 1")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(5);
    }
  }

  /** Regression test for issue #8814. */
  @Test
  void nonSetOperationsKeepTheBarrier() {
    commitAutoStartedTransaction();
    load();
    try (final ResultSet rs = database.query("sql", "EXPLAIN UPDATE V REMOVE b WHERE a BETWEEN 0 AND 10")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("MATERIALIZE");
    }
  }

  /** Regression test for issue #8814. */
  @Test
  void returnAfterAndCompoundIndex() {
    commitAutoStartedTransaction();
    database.transaction(() -> {
      final var type = database.getSchema().createVertexType("W");
      type.createProperty("a", Type.LONG);
      type.createProperty("c", Type.LONG);
      database.getSchema().buildTypeIndex("W", new String[] { "a", "c" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
      for (int i = 0; i < 500; i++)
        database.newVertex("W").set("a", (long) i, "c", 1L).save();
    });
    database.begin();
    long rows = 0;
    try (final ResultSet rs = database.command("sql", "UPDATE W SET a = a + 10, c = c + 1 RETURN AFTER WHERE a >= 0 AND a <= 100")) {
      while (rs.hasNext()) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2);
        ++rows;
      }
    }
    database.commit();
    assertThat(rows).isEqualTo(101);
  }

  /** Regression test for issue #8814. */
  @Test
  void barrierOnlyWhenTheStatementRewritesTheWalkedKey() {
    commitAutoStartedTransaction();
    load();
    try (final ResultSet rs = database.query("sql", "EXPLAIN UPDATE V SET b = 5 WHERE a BETWEEN 0 AND 10")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).doesNotContain("MATERIALIZE");
    }
    try (final ResultSet rs = database.query("sql", "EXPLAIN UPDATE V SET a = a + 1 WHERE a BETWEEN 0 AND 10")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("MATERIALIZE");
    }
  }

  // ---- #9971: UPDATE MERGE skips metadata keys ----
  //
  // Regression for issue #9971: {@code UPDATE ... MERGE} applied its payload one positional {@code set(name, value)} at a time,
  // so it refused a reserved metadata key ({@code @rid}, {@code @type}, {@code @cat}) that every other map-shaped surface -
  // {@code UPDATE ... CONTENT}, {@code INSERT ... CONTENT}, {@code MutableDocument.set(Map)}, {@code fromMap} and
  // {@code merge(Map)} - skips. A document read back as JSON carries those keys, so it could no longer be merged back.

  private void createMergeTypes() {
    commitAutoStartedTransaction();
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE D1");
      database.command("sql", "CREATE VERTEX TYPE V1");
      database.command("sql", "CREATE EDGE TYPE E1");
      database.command("sql", "INSERT INTO D1 SET q = 1");
    });
  }

  /** Regression test for issue #9971. */
  @Test
  void aReservedKeyIsSkippedAsByEveryOtherMapSurface() {
    createMergeTypes();
    database.transaction(() -> database.command("sql", "UPDATE D1 MERGE {\"@type\":\"HACKED\",\"@cat\":\"v\",\"k2\":\"v2\"}"));
    final Document doc = single();
    assertThat(doc.getTypeName()).isEqualTo("D1");
    assertThat(doc.toMap(false)).containsOnly(Map.entry("q", 1), Map.entry("k2", "v2"));
  }

  /** Regression test for issue #9971. */
  @Test
  void aDocumentReadBackAsJsonMergesBack() {
    createMergeTypes();
    final String json = single().toJSON(true).put("k3", "v3").toString();
    assertThat(json).contains("@rid").contains("@type").contains("@cat");

    database.transaction(() -> database.command("sql", "UPDATE D1 MERGE " + json));
    assertThat(single().toMap(false)).containsOnly(Map.entry("q", 1), Map.entry("k3", "v3"));
  }

  /**
   * Regression test for issue #9971.
   * <p>
   * {@code "@rid"} is lexed as a token of its own, for the {@code {"@rid": expr}} RID form, so no map literal could carry it
   * as a key: every CONTENT and MERGE of a document read back as JSON failed to parse before reaching the map contract.
   */
  @Test
  void aMapLiteralCanCarryTheRidKey() {
    createMergeTypes();
    final String rid = single().getIdentity().toString();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO D1 CONTENT {\"@rid\":\"" + rid + "\",\"@type\":\"D1\",\"z\":1}");
      database.command("sql", "UPDATE D1 CONTENT {\"@rid\":\"#99:99\",\"q\":5} WHERE q = 1");
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM D1 ORDER BY q")) {
      assertThat(rs.next().toElement().toMap(false)).containsOnly(Map.entry("z", 1));
      final Document updated = rs.next().toElement();
      assertThat(updated.getIdentity()).hasToString(rid);
      assertThat(updated.toMap(false)).containsOnly(Map.entry("q", 5));
    }

    // a map literal with the key reads as a map everywhere a map is read, and the single-entry RID form is still a RID
    try (final ResultSet rs = database.query("sql",
        "SELECT {\"@rid\": 1, \"a\": 2} AS m1, {\"a\": 1, \"@rid\": 2, \"b\": 3} AS m2, {\"@rid\": \"" + rid + "\"} AS r")) {
      final Result row = rs.next();
      assertThat(row.<Map<String, Object>>getProperty("m1")).containsOnly(Map.entry("@rid", 1), Map.entry("a", 2));
      assertThat(row.<Map<String, Object>>getProperty("m2")).containsOnly(Map.entry("a", 1), Map.entry("@rid", 2),
          Map.entry("b", 3));
      assertThat(row.<Object>getProperty("r")).isInstanceOf(Identifiable.class);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM {\"@rid\": \"" + rid + "\"}")) {
      assertThat(rs.next().getIdentity()).hasValueSatisfying(r -> assertThat(r).hasToString(rid));
    }
  }

  /** Regression test for issue #9971. */
  @Test
  void aNullValueAndASchemaTypeAreAppliedAsByThePositionalSetter() {
    createMergeTypes();
    database.transaction(() -> {
      database.command("sql", "CREATE PROPERTY D1.l LONG");
      database.command("sql", "UPDATE D1 SET gone = 'x'");
      database.command("sql", "UPDATE D1 MERGE {\"l\": 5, \"gone\": null, \"nested\": {\"a\": 1, \"b\": [1, 2]}}");
    });
    final Document doc = single();
    assertThat(doc.get("l")).isInstanceOf(Long.class).isEqualTo(5L);
    assertThat(doc.has("gone")).isTrue();
    assertThat(doc.get("gone")).isNull();
    assertThat(doc.get("nested")).isInstanceOf(Map.class);
    assertThat((Map<String, Object>) doc.get("nested")).containsEntry("a", 1).containsEntry("b", List.of(1, 2));
  }

  /** Regression test for issue #9971. */
  @Test
  void aParameterPayloadWithMetadataMergesToo() {
    createMergeTypes();
    database.transaction(() -> database.command("sql", "UPDATE D1 MERGE :payload",
        Map.of("payload", Map.of("@rid", "#99:99", "@type", "HACKED", "k4", 4))));
    assertThat(single().toMap(false)).containsOnly(Map.entry("q", 1), Map.entry("k4", 4));
  }

  /** Regression test for issue #9971. */
  @Test
  void anEdgeIsStillRePointedThroughMerge() {
    createMergeTypes();
    final RID[] rids = new RID[4];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V1").set("n", "a").save();
      final MutableVertex b = database.newVertex("V1").set("n", "b").save();
      final MutableVertex c = database.newVertex("V1").set("n", "c").save();
      rids[0] = a.newEdge("E1", b, "w", 1).getIdentity();
      rids[1] = a.getIdentity();
      rids[2] = b.getIdentity();
      rids[3] = c.getIdentity();
    });

    database.transaction(() -> database.command("sql",
        "UPDATE " + rids[0] + " MERGE {\"@in\":\"" + rids[3] + "\",\"@type\":\"E1\",\"w\":2}"));

    // RE-POINTING REPLACES THE EDGE RECORD (#5779): READ THE ONE EDGE OF THE TYPE BACK
    final Edge edge;
    try (final ResultSet rs = database.query("sql", "SELECT FROM E1")) {
      edge = rs.next().getEdge().orElseThrow();
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(edge.getIn()).isEqualTo(rids[3]);
    assertThat(edge.getOut()).isEqualTo(rids[1]);
    assertThat(edge.getInteger("w")).isEqualTo(2);
  }

  private Document single() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM D1")) {
      assertThat(rs.hasNext()).isTrue();
      final Document doc = rs.next().toElement();
      assertThat(rs.hasNext()).isFalse();
      return doc;
    }
  }

  // ---- #10194: UPDATE SET reads the row as it was ----
  //
  // #10194, first point: {@code UPDATE ... SET} assigned its items left to right on the record being updated, so a later item saw
  // the new value of an earlier one and {@code SET a = b, b = a} left both columns equal instead of swapping them. In SQL every
  // right-hand side reads the row as it was before the statement.

  private void createSwapRow() {
    commitAutoStartedTransaction();
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.a INTEGER").close();
    database.command("sql", "CREATE PROPERTY T.b INTEGER").close();
    database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 1, a = 1, b = 2").close());
  }

  private void update(final String statement) {
    database.transaction(() -> database.command("sql", statement).close());
  }

  private void assertRow(final int a, final int b) {
    try (final ResultSet rs = database.query("sql", "SELECT a, b FROM T WHERE id = 1")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("a").intValue()).as("a").isEqualTo(a);
      assertThat(row.<Number>getProperty("b").intValue()).as("b").isEqualTo(b);
    }
  }

  /** Regression test for issue #10194. */
  @Test
  void swapExchangesTheValues() {
    createSwapRow();
    update("UPDATE T SET a = b, b = a");

    assertRow(2, 1);
  }

  /** Regression test for issue #10194. */
  @Test
  void aLaterItemReadsTheOldValueOfAnEarlierOne() {
    createSwapRow();
    update("UPDATE T SET b = 7, a = b");

    assertRow(2, 7);
  }

  /** Regression test for issue #10194. */
  @Test
  void compoundOperatorsReadTheOldRow() {
    createSwapRow();
    update("UPDATE T SET a += b, b += a");

    assertRow(3, 3);
  }

  /** Regression test for issue #10194. */
  @Test
  void twoItemsOnTheSamePropertyStillApplyInOrder() {
    createSwapRow();
    // only the right-hand sides are read from the old row: a compound operator extends the value its own property holds by then
    update("UPDATE T SET a = 1, a += 2");

    assertRow(3, 2);
  }

  /** Regression test for issue #10194. */
  @Test
  void aRecordBeingCreatedHasNoRowToRead() {
    createSwapRow();
    // a record that is being created has no row as it was: its items are evaluated against nothing, as before this change
    // (INSERT and CREATE EDGE build their record from the evaluated items), so the snapshot is for existing records only
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE E").close();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX V SET n = 1").close();
      database.command("sql", "CREATE VERTEX V SET n = 2").close();
    });

    update("CREATE EDGE E FROM (SELECT FROM V WHERE n = 1) TO (SELECT FROM V WHERE n = 2) SET w = 5, z = w");

    try (final ResultSet rs = database.query("sql", "SELECT w, z FROM E")) {
      final Result edge = rs.next();
      assertThat(edge.<Number>getProperty("w").intValue()).isEqualTo(5);
      assertThat(edge.<Object>getProperty("z")).isNull();
    }
  }

  /** Regression test for issue #10194. */
  @Test
  void expressionsReadTheOldRow() {
    createSwapRow();
    update("UPDATE T SET a = a + b, b = a * 10");

    assertRow(3, 10);
  }

  /** Regression test for issue #10194. */
  @Test
  void returnAfterShowsTheSwappedRow() {
    createSwapRow();
    final int[] seen = new int[2];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET a = b, b = a RETURN AFTER")) {
        final Result row = rs.next();
        seen[0] = row.<Number>getProperty("a").intValue();
        seen[1] = row.<Number>getProperty("b").intValue();
      }
    });

    assertThat(seen).containsExactly(2, 1);
    assertRow(2, 1);
  }

  /** Regression test for issue #10194. */
  @Test
  void returnBeforeShowsTheOriginalRow() {
    createSwapRow();
    final int[] seen = new int[2];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET a = b, b = a RETURN BEFORE")) {
        final Result row = rs.next();
        seen[0] = row.<Number>getProperty("a").intValue();
        seen[1] = row.<Number>getProperty("b").intValue();
      }
    });

    assertThat(seen).containsExactly(1, 2);
  }

  /** Regression test for issue #10194. */
  @Test
  void swapAppliesToEveryRowOfTheStatement() {
    createSwapRow();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 2, a = 10, b = 20").close();
      database.command("sql", "INSERT INTO T SET id = 3, a = 30, b = 40").close();
    });

    update("UPDATE T SET a = b, b = a");

    try (final ResultSet rs = database.query("sql", "SELECT id, a, b FROM T ORDER BY id")) {
      final int[][] expected = { { 1, 2, 1 }, { 2, 20, 10 }, { 3, 40, 30 } };
      for (final int[] e : expected) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(e[0]);
        assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(e[1]);
        assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(e[2]);
      }
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Regression test for issue #10194. */
  @Test
  void swapKeepsTheIndexesInSync() {
    createSwapRow();
    database.command("sql", "CREATE INDEX ON T (a) NOTUNIQUE").close();
    database.command("sql", "CREATE INDEX ON T (b) NOTUNIQUE").close();

    update("UPDATE T SET a = b, b = a");

    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE a = 2 AND b = 1")) {
      assertThat(rs.hasNext()).isTrue();
    }
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE a = 1")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Regression test for issue #10194. */
  @Test
  void nestedAssignmentsReadTheOldRow() {
    createSwapRow();
    database.transaction(() -> database.command("sql", "UPDATE T SET m = {'x': 1}").close());

    update("UPDATE T SET m.x = b, b = 99");

    try (final ResultSet rs = database.query("sql", "SELECT m.x AS x, b FROM T WHERE id = 1")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("x").intValue()).isEqualTo(2);
      assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(99);
    }
  }

  /** Regression test for issue #10194. */
  @Test
  void aSingleItemBehavesAsBefore() {
    createSwapRow();
    update("UPDATE T SET a = a + 1");

    assertRow(2, 2);
  }

  /** Regression test for issue #10194. */
  @Test
  void upsertSwapsToo() {
    createSwapRow();
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE").close();

    update("UPDATE T SET a = b, b = a UPSERT WHERE id = 1");

    assertRow(2, 1);
  }
}
