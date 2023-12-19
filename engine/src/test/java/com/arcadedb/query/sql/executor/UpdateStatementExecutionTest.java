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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.format.*;
import java.util.*;

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

  @Test
  public void testSetString() {
    ResultSet result = database.command("sql", "update " + className + " set surname = 'foo'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));

    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("foo", item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCopyField() {
    ResultSet result = database.command("sql", "update " + className + " set surname = name");

    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals((Object) item.getProperty("name"), item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSetExpression() {
    ResultSet result = database.command("sql", "update " + className + " set surname = 'foo'+name ");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("foo" + item.getProperty("name"), item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testConditionalSet() {
    ResultSet result = database.command("sql", "update " + className + " set surname = 'foo' where name = 'name3'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        Assertions.assertEquals("foo", item.getProperty("surname"));
        found = true;
      }
    }
    Assertions.assertTrue(found);
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSetOnList() {
    ResultSet result = database.command("sql", "update " + className + " set tagsList[0] = 'abc' where name = 'name3'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        final List<String> tags = new ArrayList<>();
        tags.add("abc");
        tags.add("bar");
        tags.add("baz");
        Assertions.assertEquals(tags, item.getProperty("tagsList"));
        found = true;
      }
    }
    Assertions.assertTrue(found);
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSetOnList2() {
    ResultSet result = database.command("sql", "update " + className + " set tagsList[6] = 'abc' where name = 'name3'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        final List<String> tags = new ArrayList<>();
        tags.add("foo");
        tags.add("bar");
        tags.add("baz");
        tags.add(null);
        tags.add(null);
        tags.add(null);
        tags.add("abc");
        Assertions.assertEquals(tags, item.getProperty("tagsList"));
        found = true;
      }
    }
    Assertions.assertTrue(found);
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSetOnMap() {
    ResultSet result = database.command("sql", "update " + className + " set tagsMap['foo'] = 'abc' where name = 'name3'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        final Map<String, String> tags = new HashMap<>();
        tags.put("foo", "abc");
        tags.put("bar", "bar");
        tags.put("baz", "baz");
        Assertions.assertEquals(tags, item.getProperty("tagsMap"));
        found = true;
      } else {
        final Map<String, String> tags = new HashMap<>();
        tags.put("foo", "foo");
        tags.put("bar", "bar");
        tags.put("baz", "baz");
        Assertions.assertEquals(tags, item.getProperty("tagsMap"));
      }
    }
    Assertions.assertTrue(found);
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testPlusAssignCollection() {
    ResultSet result = database.command("sql",
        "insert into " + className + " set listStrings = ['this', 'is', 'a', 'test'], listNumbers = [1,2,3]");
    final RID rid = result.next().getIdentity().get();
    result = database.command("sql", "update " + rid + " set listStrings += '!', listNumbers += 9");
    Assertions.assertTrue(result.hasNext());
    Assertions.assertNotNull(result.next());

    result = database.command("sql", "select from " + rid);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);

    List<String> listStrings = item.getProperty("listStrings");
    Assertions.assertEquals(5, listStrings.size());
    Assertions.assertEquals("!", listStrings.get(4));

    List<Number> listNumbers = item.getProperty("listNumbers");
    Assertions.assertEquals(4, listNumbers.size());
    Assertions.assertEquals(9, listNumbers.get(3));
  }

  @Test
  public void testPlusAssignMap() {
    ResultSet result = database.command("sql", "insert into " + className + " set map1 = {'name':'Jay'}, map2 = {'name':'Jay'}");
    final RID rid = result.next().getIdentity().get();
    result = database.command("sql", "update " + rid + " set map1 += { 'last': 'Miner'}, map2 += [ 'last', 'Miner']");
    Assertions.assertTrue(result.hasNext());
    Assertions.assertNotNull(result.next());

    result = database.command("sql", "select from " + rid);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);

    Map<String, String> map1 = item.getProperty("map1");
    Assertions.assertEquals(2, map1.size());
    Assertions.assertEquals("Jay", map1.get("name"));
    Assertions.assertEquals("Miner", map1.get("last"));

    Map<String, String> map2 = item.getProperty("map2");
    Assertions.assertEquals(2, map2.size());
    Assertions.assertEquals("Jay", map2.get("name"));
    Assertions.assertEquals("Miner", map2.get("last"));
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
    Assertions.assertTrue(result.hasNext());
    Assertions.assertNotNull(result.next());

    result = database.command("sql", "select from " + rid);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);

    Map<String, Object> map1 = item.getProperty("bars");
    Assertions.assertEquals(2, map1.size());

    Map nestedMap = (Map) map1.get("23-03-24");
    Assertions.assertNotNull(nestedMap);
    Assertions.assertEquals(100, nestedMap.get("volume"));

    nestedMap = (Map) map1.get("2023-03-08");
    Assertions.assertNotNull(nestedMap);
    Assertions.assertEquals(134, nestedMap.get("volume"));
  }

  @Test
  public void testPlusAssign() {
    ResultSet result = database.command("sql", "update " + className + " set name += 'foo', newField += 'bar', number += 5");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertTrue(item.getProperty("name").toString().endsWith("foo")); // test concatenate string to string
      Assertions.assertEquals(8, item.getProperty("name").toString().length());
      Assertions.assertEquals("bar", item.getProperty("newField")); // test concatenate null to string
      Assertions.assertEquals((Object) 9L, item.getProperty("number")); // test sum numbers
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMinusAssign() {
    ResultSet result = database.command("sql", "update " + className + " set number -= 5");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals((Object) (-1L), item.getProperty("number"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMinusAssignCollection() {
    ResultSet result = database.command("sql",
        "insert into " + className + " set listStrings = ['this', 'is', 'a', 'test'], listNumbers = [1,2,3]");
    final RID rid = result.next().getIdentity().get();
    result = database.command("sql", "update " + rid + " set listStrings -= 'a', listNumbers -= 2");
    Assertions.assertTrue(result.hasNext());
    Assertions.assertNotNull(result.next());

    result = database.command("sql", "select from " + rid);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);

    List<String> listStrings = item.getProperty("listStrings");
    Assertions.assertEquals(3, listStrings.size());
    Assertions.assertFalse(listStrings.contains("!"));

    List<Number> listNumbers = item.getProperty("listNumbers");
    Assertions.assertEquals(2, listNumbers.size());
    Assertions.assertFalse(listNumbers.contains(2));
  }

  @Test
  public void testMinusAssignMap() {
    ResultSet result = database.command("sql", "insert into " + className + " set map1 = {'name':'Jay'}, map2 = {'name':'Jay'}");
    final RID rid = result.next().getIdentity().get();
    result = database.command("sql", "update " + rid + " set map1 -= {'name':'Jay'}, map2 -= [ 'name' ]");
    Assertions.assertTrue(result.hasNext());
    Assertions.assertNotNull(result.next());

    result = database.command("sql", "select from " + rid);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);

    Map<String, String> map1 = item.getProperty("map1");
    Assertions.assertEquals(0, map1.size());

    Map<String, String> map2 = item.getProperty("map2");
    Assertions.assertEquals(0, map2.size());
  }

  @Test
  public void testStarAssign() {
    ResultSet result = database.command("sql", "update " + className + " set number *= 5");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals((Object) 20L, item.getProperty("number"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSlashAssign() {
    ResultSet result = database.command("sql", "update " + className + " set number /= 2");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals((Object) 2L, item.getProperty("number"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemove() {
    ResultSet result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      final Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertNotNull(item.getProperty("surname"));
    }

    result.close();
    result = database.command("sql", "update " + className + " remove surname");
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      final Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      final Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertFalse(item.toElement().has("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testContent() {

    ResultSet result = database.command("sql", "update " + className + " content {'name': 'foo', 'secondName': 'bar'}");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("foo", item.getProperty("name"));
      Assertions.assertEquals("bar", item.getProperty("secondName"));
      Assertions.assertNull(item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMerge() {

    ResultSet result = database.command("sql", "update " + className + " merge {'name': 'foo', 'secondName': 'bar'}");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 10L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("foo", item.getProperty("name"));
      Assertions.assertEquals("bar", item.getProperty("secondName"));
      Assertions.assertTrue(item.getProperty("surname").toString().startsWith("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpsertBucket() {
    final List<Bucket> buckets = database.getSchema().getType(className).getBuckets(false);

    // BY BUCKET ID
    ResultSet result = database.command("sql",
        "update bucket:" + buckets.getFirst().getName() + " set foo = 'bar' upsert where name = 'name1'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    // BY BUCKET ID
    result = database.command("sql",
        "update bucket:" + buckets.getFirst().getFileId() + " set foo = 'bar' upsert where name = 'name1'");
    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from bucket:" + buckets.getFirst().getName());
    Assertions.assertTrue(result.hasNext());

    while (result.hasNext()) {
      item = result.next();
      Assertions.assertNotNull(item);
      final String name = item.getProperty("name");
      Assertions.assertNotNull(name);
      if ("name1".equals(name)) {
        Assertions.assertEquals("bar", item.getProperty("foo"));
      } else {
        Assertions.assertNull(item.getProperty("foo"));
      }
    }
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.command("sql", "update bucket:" + buckets.getFirst().getName() + " remove foo upsert where name = 'name1'");
    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from bucket:" + buckets.getFirst().getName());
    Assertions.assertTrue(result.hasNext());

    while (result.hasNext()) {
      item = result.next();
      Assertions.assertNotNull(item);
      final String name = item.getProperty("name");
      Assertions.assertNotNull(name);
      Assertions.assertNull(item.getProperty("foo"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpsert1() {
    ResultSet result = database.command("sql", "update " + className + " set foo = 'bar' upsert where name = 'name1'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      final String name = item.getProperty("name");
      Assertions.assertNotNull(name);
      if ("name1".equals(name)) {
        Assertions.assertEquals("bar", item.getProperty("foo"));
      } else {
        Assertions.assertNull(item.getProperty("foo"));
      }
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/380
   */
  @Test
  public void testUpsertVertices() {
    database.command("sql", "CREATE vertex TYPE extra_node");
    database.command("sql", "CREATE PROPERTY extra_node.extraitem STRING");
    database.command("sql", "CREATE INDEX ON extra_node (extraitem) UNIQUE");
    final ResultSet result = database.command("sql",
        "update extra_node set extraitem = 'Hugo2' upsert return after $current where extraitem = 'Hugo'");

    Assertions.assertTrue(result.hasNext());
    final Result item = result.next();
    Assertions.assertNotNull(item);
    final Vertex current = item.getProperty("$current");
    Assertions.assertEquals("Hugo2", current.getString("extraitem"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpsertAndReturn() {
    final ResultSet result = database.command("sql",
        "update " + className + " set foo = 'bar' upsert  return after  where name = 'name1' ");

    Assertions.assertTrue(result.hasNext());
    final Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals("bar", item.getProperty("foo"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpsert2() {

    ResultSet result = database.command("sql", "update " + className + " set foo = 'bar' upsert where name = 'name11'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    for (int i = 0; i < 11; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      final String name = item.getProperty("name");
      Assertions.assertNotNull(name);
      if ("name11".equals(name)) {
        Assertions.assertEquals("bar", item.getProperty("foo"));
      } else {
        Assertions.assertNull(item.getProperty("foo"));
      }
    }
    Assertions.assertFalse(result.hasNext());
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
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertNotNull(item);
    final List ls = item.getProperty("theProperty");
    Assertions.assertNotNull(ls);
    Assertions.assertEquals(9, ls.size());
    Assertions.assertFalse(ls.contains("n0"));
    Assertions.assertFalse(result.hasNext());
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
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertNotNull(item);
    final List ls = item.getProperty("theProperty");

    Assertions.assertNotNull(ls);
    Assertions.assertEquals(ls.size(), 7);
    Assertions.assertFalse(ls.contains("n0"));
    Assertions.assertFalse(ls.contains("n1"));
    Assertions.assertTrue(ls.contains("n2"));
    Assertions.assertFalse(ls.contains("n3"));
    Assertions.assertTrue(ls.contains("n4"));

    Assertions.assertFalse(result.hasNext());
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
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from " + className);
    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertNotNull(item);
    final EmbeddedDocument ls = item.getProperty("theProperty");
    Assertions.assertNotNull(ls);
    Assertions.assertFalse(ls.getPropertyNames().contains("sub"));
    Assertions.assertEquals("bar", ls.getString("aaa"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemoveFromMapSquare() {

    database.command("sql", "UPDATE " + className + " REMOVE tagsMap[\"bar\"]").close();

    final ResultSet result = database.query("sql", "SELECT tagsMap FROM " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      final Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals(2, ((Map) item.getProperty("tagsMap")).size());
      Assertions.assertFalse(((Map) item.getProperty("tagsMap")).containsKey("bar"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemoveFromMapEquals() {

    database.command("sql", "UPDATE " + className + " REMOVE tagsMap = \"bar\"").close();

    final ResultSet result = database.query("sql", "SELECT tagsMap FROM " + className);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      final Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals(2, ((Map) item.getProperty("tagsMap")).size());
      Assertions.assertFalse(((Map) item.getProperty("tagsMap")).containsKey("bar"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testReturnBefore() {
    final ResultSet result = database.command("sql",
        "update " + className + " set name = 'foo' RETURN BEFORE where name = 'name1'");
    Assertions.assertTrue(result.hasNext());
    final Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals("name1", item.getProperty("name"));

    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void upsertVertices() {
    database.getSchema().createVertexType("UpsertableVertex");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newVertex("UpsertableVertex");
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

    ResultSet result = database.command("sql", "update UpsertableVertex set foo = 'bar' upsert where name = 'name1'");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals((Object) 1L, item.getProperty("count"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    result = database.query("sql", "SElect from UpsertableVertex");
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      item = result.next();
      Assertions.assertNotNull(item);
      final String name = item.getProperty("name");
      Assertions.assertNotNull(name);
      if ("name1".equals(name)) {
        Assertions.assertEquals("bar", item.getProperty("foo"));
      } else {
        Assertions.assertNull(item.getProperty("foo"));
      }
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLocalDateTimeUpsertWithIndex() throws ClassNotFoundException {
    if (System.getProperty("os.name").startsWith("Windows"))
      // NOTE: ON WINDOWS MICROSECONDS ARE NOT HANDLED CORRECTLY
      return;

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

    final LocalDateTime start = LocalDateTime.now();

    for (int i = 0; i < 10; i++) {
      database.transaction(() -> {
        final LocalDateTime stop = LocalDateTime.now();
        database.command("sql", "UPDATE Product SET start = ?, stop = ? UPSERT WHERE start = ? and stop = ?", start, stop);

        Result result;
        ResultSet resultSet = database.query("sql", "SELECT from Product");
        while (resultSet.hasNext()) {
          result = resultSet.next();
          Assertions.assertNotNull(result.getProperty("start"));
          Assertions.assertNotNull(result.getProperty("stop"));
        }
      });
    }
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

        Assertions.assertTrue(resultSet.hasNext());

        while (resultSet.hasNext()) {
          result = resultSet.next();
          Assertions.assertNotNull(result.getProperty("start"));
          Assertions.assertNotNull(result.getProperty("stop"));
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
    String status = "PENDING";
    for (int i = 0; i < 2; i++) {
      int id = i + 1;
      database.transaction(() -> {
        String sqlString = "INSERT INTO Order SET id = ?, status = ?, processor = ?";
        try (ResultSet resultSet1 = database.command("sql", sqlString, id, status, processor)) {
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
      Object[] parameters2 = { "PENDING" };
      String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
      try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
        final Result record = resultSet1.next();
        Assertions.assertEquals("PENDING", record.getProperty("status"));
        Assertions.assertEquals("2", record.getProperty("id"));
      }
    });
    // drop index
    database.getSchema().dropIndex(typeIndex[0].getName());

    // repeat select records with status = 'PENDING'
    database.transaction(() -> {
      Object[] parameters2 = { "PENDING" };
      String sqlString = "SELECT id, processor, status FROM Order WHERE status = ?";
      try (ResultSet resultSet1 = database.query("sql", sqlString, parameters2)) {
        Assertions.assertEquals("PENDING", resultSet1.next().getProperty("status"));
      }
    });

    database.transaction(() -> {
      Object[] parameters2 = { "PENDING" };
      try (ResultSet resultSet1 = database.query("sql",
          "SELECT id, status FROM Order WHERE status = 'PENDING' OR status = 'READY' ORDER BY id ASC LIMIT 1")) {
        Assertions.assertEquals("PENDING", resultSet1.next().getProperty("status"));
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
      Assertions.assertTrue(resultSet.hasNext());
    }
    database.commit();
    // update order
    database.begin();
    String UPDATE_ORDER = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
    try (ResultSet resultSet = database.command("sql", UPDATE_ORDER, "ERROR", 1)) {
      Assertions.assertTrue(resultSet.hasNext());
    }
    database.commit();
    // resubmit order 1
    database.begin();
    // "UPDATE Order SET status = ? WHERE (status != ? AND status != ?) AND id = ?";
    String RESUBMIT_ORDER = "UPDATE Order SET status = ? RETURN AFTER WHERE (status != ? AND status != ?) AND id = ?";
    try (ResultSet resultSet = database.command("sql", RESUBMIT_ORDER, "PENDING", "PENDING", "PROCESSING", 1)) {
      Assertions.assertTrue(resultSet.hasNext());
    }
    database.commit();
    // retrieve order 1 by id
    try (ResultSet resultSet = database.query("sql", "SELECT FROM Order WHERE id = 1")) {
      Assertions.assertTrue(resultSet.hasNext());
    }
    // retrieve order 1 by status
    String RETRIEVE_NEXT_ELIGIBLE_ORDERS = "SELECT id, status FROM Order WHERE status = 'PENDING' OR status = 'READY' ORDER BY id ASC LIMIT 1";
    try (ResultSet resultSet = database.query("sql", RETRIEVE_NEXT_ELIGIBLE_ORDERS)) {
      Assertions.assertTrue(resultSet.hasNext());
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
        Assertions.assertTrue(resultSet.hasNext());
        while (resultSet.hasNext())
          Assertions.assertEquals("bob", resultSet.next().getProperty("name"));
      });
    }
  }

}
