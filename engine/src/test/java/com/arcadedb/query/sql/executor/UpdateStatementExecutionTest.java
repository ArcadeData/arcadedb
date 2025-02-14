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
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.format.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

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
    database.transaction(() -> {
      database.command("sql", "delete from OUpdateStatementExecutionTest");
    });
  }

  @Test
  public void testSetString() {
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
  public void testCopyField() {
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
  public void testSetExpression() {
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
  public void testConditionalSet() {
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
  public void testSetOnList() {
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
  public void testSetOnList2() {
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
  public void testSetOnMap() {
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
  public void testPlusAssignCollection() {
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
  public void testPlusAssignMap() {
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
  public void testPlusAssignNestedMaps() {
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
  public void testPlusAssign() {
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
  public void testMinusAssign() {
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
  public void testMinusAssignCollection() {
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
  public void testMinusAssignMap() {
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
  public void testStarAssign() {
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
  public void testSlashAssign() {
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
  public void testRemove() {
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
  public void testContent() {

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
  public void testMerge() {

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
      assertThat(item.<String>getProperty("surname").toString().startsWith("surname")).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testRemove1() {
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
  public void testRemove2() {
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
  public void testRemove3() {
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
  public void testRemoveFromMapSquare() {

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
  public void testRemoveFromMapEquals() {

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
  public void testReturnBefore() {
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
  public void testLocalDateTimeUpsertWithIndexMicros() throws ClassNotFoundException {
    database.transaction(() -> {
      if (database.getSchema().existsType("Product"))
        database.getSchema().dropType("Product");

      DocumentType dtProduct = database.getSchema().createDocumentType("Product");
      dtProduct.createProperty("start", Type.DATETIME_MICROS);
      dtProduct.createProperty("stop", Type.DATETIME_MICROS);
      dtProduct.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "start", "stop");
    });

    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(java.time.LocalDateTime.class);
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(java.time.LocalDateTime.class);

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
  public void testCompositeIndexLookup() {
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(java.time.LocalDateTime.class);
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
      Object[] parameters2 = { "ERROR", 1 };
      String sqlString = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
      try (ResultSet resultSet1 = database.command("sql", sqlString, parameters2)) {
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
      Object[] parameters2 = { "PENDING" };
      String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
      try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
        assertThat(resultSet1.next().<String>getProperty("status")).isEqualTo("PENDING");
      }
    });

    database.transaction(() -> {
      Object[] parameters2 = { "PENDING" };
      try (ResultSet resultSet1 = database.query("sql",
          "SELECT id, status FROM Order WHERE status = 'PENDING' OR status = 'READY' ORDER BY id ASC LIMIT 1")) {
        assertThat(resultSet1.next().<String>getProperty("status")).isEqualTo("PENDING");
      }
    });
  }

  @Test
  public void testSelectAfterUpdate() {
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

}
