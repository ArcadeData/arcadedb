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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class InsertStatementExecutionTest extends TestHelper {
  public InsertStatementExecutionTest() {
    autoStartTx = true;
  }

  @Test
  public void testInsertSet() {
    String className = "testInsertSet";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + " set name = 'name1'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
    }
    Assertions.assertFalse(result.hasNext());

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertValue() {
    String className = "testInsertValue";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + "  (name, surname) values ('name1', 'surname1')");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
      Assertions.assertEquals("surname1", item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertValue2() {
    String className = "testInsertValue2";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + "  (name, surname) values ('name1', 'surname1'), ('name2', 'surname2')");
    printExecutionPlan(result);

    for (int i = 0; i < 2; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name" + (i + 1), item.getProperty("name"));
      Assertions.assertEquals("surname" + (i + 1), item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    names.add("name1");
    names.add("name2");
    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 2; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assertions.assertNotNull(item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    Assertions.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testInsertFromSelect1() {
    String className1 = "testInsertFromSelect1";
    database.getSchema().createDocumentType(className1);

    String className2 = "testInsertFromSelect1_1";
    database.getSchema().createDocumentType(className2);
    for (int i = 0; i < 10; i++) {
      MutableDocument doc = database.newDocument(className1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    ResultSet result = database.command("sql", "insert into " + className2 + " from select from " + className1);
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertNotNull(item.getProperty("name"));
      Assertions.assertNotNull(item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      names.add("name" + i);
    }
    result = database.query("sql", "select from " + className2);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assertions.assertNotNull(item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    Assertions.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testInsertFromSelect2() {
    String className1 = "testInsertFromSelect2";
    database.getSchema().createDocumentType(className1);

    String className2 = "testInsertFromSelect2_1";
    database.getSchema().createDocumentType(className2);
    for (int i = 0; i < 10; i++) {
      MutableDocument doc = database.newDocument(className1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    ResultSet result = database.command("sql", "insert into " + className2 + " ( select from " + className1 + ")");
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertNotNull(item.getProperty("name"));
      Assertions.assertNotNull(item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      names.add("name" + i);
    }
    result = database.query("sql", "select from " + className2);
    for (int i = 0; i < 10; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assertions.assertNotNull(item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    Assertions.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testContent() {
    String className = "testContent";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + " content {'name':'name1', 'surname':'surname1'}");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
    }
    Assertions.assertFalse(result.hasNext());

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
      Assertions.assertEquals("surname1", item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testContentWithParam() {
    String className = "testContentWithParam";
    database.getSchema().createDocumentType(className);

    Map<String, Object> theContent = new HashMap<>();
    theContent.put("name", "name1");
    theContent.put("surname", "surname1");
    Map<String, Object> params = new HashMap<>();
    params.put("theContent", theContent);
    ResultSet result = database.command("sql", "insert into " + className + " content :theContent", params);
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
    }
    Assertions.assertFalse(result.hasNext());

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
      Assertions.assertEquals("surname1", item.getProperty("surname"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLinkConversion() {
    String className1 = "testLinkConversion1";
    String className2 = "testLinkConversion2";

    database.command("sql", "create document type " + className1).close();
    database.command("sql", "INSERT INTO " + className1 + " SET name='Active';").close();
    database.command("sql", "INSERT INTO " + className1 + " SET name='Inactive';").close();

    database.command("sql", "create document type " + className2 + ";").close();
    database.command("sql", "CREATE PROPERTY " + className2 + ".processingType LINK " + className1 + ";").close();

    database.command("sql", "INSERT INTO " + className2 + " SET name='Active', processingType = (SELECT FROM " + className1 + " WHERE name = 'Active') ;")
        .close();
    database.command("sql", "INSERT INTO " + className2 + " SET name='Inactive', processingType = (SELECT FROM " + className1 + " WHERE name = 'Inactive') ;")
        .close();

    ResultSet result = database.query("sql", "seLECT FROM " + className2);
    for (int i = 0; i < 2; i++) {
      Assertions.assertTrue(result.hasNext());
      Result row = result.next();
      Object val = row.getProperty("processingType");
      Assertions.assertNotNull(val);
      Assertions.assertTrue(val instanceof Identifiable);
    }
    result.close();
  }

  @Test
  public void testLISTConversion() {
    String className1 = "testLISTConversion1";
    String className2 = "testLISTConversion2";

    database.command("sql", "create document type " + className1).close();

    database.command("sql", "create document type " + className2 + ";").close();
    database.command("sql", "CREATE PROPERTY " + className2 + ".sub LIST;").close();

    database.command("sql", "INSERT INTO " + className2 + " SET name='Active', sub = [{'name':'foo'}];").close();

    ResultSet result = database.query("sql", "seLECT FROM " + className2);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result row = result.next();
      Object list = row.getProperty("sub");
      Assertions.assertNotNull(list);
      Assertions.assertTrue(list instanceof List);
      Assertions.assertEquals(1, ((List) list).size());

      Object o = ((List) list).get(0);
      Assertions.assertTrue(o instanceof Map);
      Assertions.assertEquals("foo", ((Map) o).get("name"));
    }
    result.close();
  }

  @Test
  public void testLISTConversion2() {
    String className1 = "testLISTConversion21";
    String className2 = "testLISTConversion22";

    database.command("sql", "create document type " + className1).close();

    database.command("sql", "create document type " + className2 + ";").close();
    database.command("sql", "CREATE PROPERTY " + className2 + ".sub LIST;").close();

    database.command("sql", "INSERT INTO " + className2 + " (name, sub) values ('Active', [{'name':'foo'}]);").close();

    ResultSet result = database.query("sql", "seLECT FROM " + className2);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result row = result.next();
      Object list = row.getProperty("sub");
      Assertions.assertNotNull(list);
      Assertions.assertTrue(list instanceof List);
      Assertions.assertEquals(1, ((List) list).size());

      Object o = ((List) list).get(0);
      Assertions.assertTrue(o instanceof Map);
      Assertions.assertEquals("foo", ((Map) o).get("name"));
    }
    result.close();
  }

  @Test
  public void testInsertReturn() {
    String className = "testInsertReturn";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + " set name = 'name1' RETURN 'OK' as result");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("OK", item.getProperty("result"));
    }
    Assertions.assertFalse(result.hasNext());

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertNotNull(item);
      Assertions.assertEquals("name1", item.getProperty("name"));
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNestedInsert() {
    String className = "testNestedInsert";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql",
        "insert into " + className + " set name = 'parent', children = (INSERT INTO " + className + " SET name = 'child')");

    result.close();

    result = database.query("sql", "seLECT FROM " + className);

    for (int i = 0; i < 2; i++) {
      Result item = result.next();
      if (item.getProperty("name").equals("parent")) {
        Assertions.assertTrue(item.getProperty("children") instanceof Collection);
        Assertions.assertEquals(1, ((Collection) item.getProperty("children")).size());
      }
    }
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLinkMapWithSubqueries() {
    String className = "testLinkMapWithSubqueries";
    String itemclassName = "testLinkMapWithSubqueriesTheItem";

    database.command("sql", "create document type " + className);
    database.command("sql", "create document type " + itemclassName);
    database.command("sql", "CREATE PROPERTY " + className + ".mymap MAP " + itemclassName);

    database.command("sql", "INSERT INTO " + itemclassName + " (name) VALUES ('test')");
    database.command("sql", "INSERT INTO " + className + " (mymap) VALUES ({'A-1': (SELECT FROM " + itemclassName + " WHERE name = 'test')})");

    ResultSet result = database.query("sql", "seLECT FROM " + className);

    Result item = result.next();
    Map theMap = item.getProperty("mymap");
    Assertions.assertEquals(1, theMap.size());
    Assertions.assertNotNull(theMap.get("A-1"));

    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testQuotedCharactersInJson() {
    String className = "testQuotedCharactersInJson";

    database.command("sql", "create document type " + className);

    database.command("sql", "INSERT INTO " + className + " CONTENT { name: \"jack\", memo: \"this is a \\n multi line text\" }");

    ResultSet result = database.query("sql", "seLECT FROM " + className);

    Result item = result.next();
    String memo = item.getProperty("memo");
    Assertions.assertEquals("this is a \n multi line text", memo);

    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  private void printExecutionPlan(ResultSet result) {
    //ExecutionPlanPrintUtils.printExecutionPlan(result);
  }
}
