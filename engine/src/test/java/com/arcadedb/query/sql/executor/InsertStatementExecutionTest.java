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
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class InsertStatementExecutionTest extends TestHelper {
  public InsertStatementExecutionTest() {
    autoStartTx = true;
  }

  @Test
  void insertSet() {
    final String className = "testInsertSet";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + " set name = 'name1'");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void insertValue() {
    final String className = "testInsertValue";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + "  (name, surname) values ('name1', 'surname1')");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname1");
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void insertValue2() {
    final String className = "testInsertValue2";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql",
        "insert into " + className + "  (name, surname) values ('name1', 'surname1'), ('name2', 'surname2')");

    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name" + (i + 1));
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname" + (i + 1));
    }
    assertThat(result.hasNext()).isFalse();

    final Set<String> names = new HashSet<>();
    names.add("name1");
    names.add("name2");
    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isNotNull();
      names.remove(item.<String>getProperty("name"));
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(names.isEmpty()).isTrue();
    result.close();
  }

  @Test
  void insertFromSelect1() {
    final String className1 = "testInsertFromSelect1";
    database.getSchema().createDocumentType(className1);

    final String className2 = "testInsertFromSelect1_1";
    database.getSchema().createDocumentType(className2);
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    ResultSet result = database.command("sql", "insert into " + className2 + " from select from " + className1);

    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();

    final Set<String> names = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      names.add("name" + i);
    }
    result = database.query("sql", "select from " + className2);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isNotNull();
      names.remove(item.<String>getProperty("name"));
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(names.isEmpty()).isTrue();
    result.close();
  }

  @Test
  void insertFromSelect2() {
    final String className1 = "testInsertFromSelect2";
    database.getSchema().createDocumentType(className1);

    final String className2 = "testInsertFromSelect2_1";
    database.getSchema().createDocumentType(className2);
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    ResultSet result = database.command("sql", "insert into " + className2 + " ( select from " + className1 + ")");

    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();

    final Set<String> names = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      names.add("name" + i);
    }
    result = database.query("sql", "select from " + className2);
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isNotNull();
      names.remove(item.<String>getProperty("name"));
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(names.isEmpty()).isTrue();
    result.close();
  }

  @Test
  void insertFromSelectRawValue() {
    final String className1 = "testInsertFromSelectRawValue";
    database.getSchema().createDocumentType(className1);

    ResultSet result = database.command("sql", "insert into " + className1 + " set test = ( select 777 )");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    List<Integer> list = item.getProperty("test");
    assertThat(list.size()).isEqualTo(1);
    assertThat(list.get(0)).isEqualTo(777);
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void insertFromSelectRawValues() {
    final String className1 = "testInsertFromSelectRawValues";
    database.getSchema().createDocumentType(className1);

    ResultSet result = database.command("sql", "insert into " + className1 + " set test = ( select 777, 888 )");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    List<Map> list = item.getProperty("test");
    assertThat(list.size()).isEqualTo(1);
    Map<String, Integer> map = list.get(0);
    assertThat(map.get("777")).isEqualTo(777);
    assertThat(map.get("888")).isEqualTo(888);
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void content() {
    final String className = "testContent";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + " content {'name':'name1', 'surname':'surname1'}");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void contentJsonArray() {
    final String className = "testContentArray";
    database.getSchema().createDocumentType(className, 1);

    String array = "[";
    for (int i = 0; i < 1000; i++) {
      if (i > 0)
        array += ",";
      array += "{'name':'name" + i + "', 'surname':'surname" + i + "'}";
    }
    array += "]";

    ResultSet result = database.command("sql", "insert into " + className + " content " + array);

    for (int i = 0; i < 1000; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name").toString()).isEqualTo("name" + i);
      assertThat(item.getProperty("surname").toString()).isEqualTo("surname" + i);
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);

    for (int i = 0; i < 1000; i++) {
      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name").toString()).isEqualTo("name" + i);
      assertThat(item.getProperty("surname").toString()).isEqualTo("surname" + i);
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void contentEmbedded() {
    final String className = "testContent";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql",
        "insert into " + className + " content { 'embedded': { '@type':'testContent', 'name':'name1', 'surname':'surname1'} }");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      EmbeddedDocument embedded = item.getProperty("embedded");
      assertThat(embedded.getString("name")).isEqualTo("name1");
      assertThat(embedded.getString("surname")).isEqualTo("surname1");
    }
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void contentWithParam() {
    final String className = "testContentWithParam";
    database.getSchema().createDocumentType(className);

    final Map<String, Object> theContent = new HashMap<>();
    theContent.put("name", "name1");
    theContent.put("surname", "surname1");
    final Map<String, Object> params = new HashMap<>();
    params.put("theContent", theContent);
    ResultSet result = database.command("sql", "insert into " + className + " content :theContent", params);

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void linkConversion() {
    final String className1 = "testLinkConversion1";
    final String className2 = "testLinkConversion2";

    database.command("sql", "create document type " + className1).close();
    database.command("sql", "INSERT INTO " + className1 + " SET name='Active';").close();
    database.command("sql", "INSERT INTO " + className1 + " SET name='Inactive';").close();

    database.command("sql", "create document type " + className2 + ";").close();
    database.command("sql", "CREATE PROPERTY " + className2 + ".processingType LINK;").close();

    database.command("sql", "INSERT INTO " + className2 + " SET name='Active', processingType = (SELECT FROM " + className1
        + " WHERE name = 'Active') ;").close();
    database.command("sql", "INSERT INTO " + className2 + " SET name='Inactive', processingType = (SELECT FROM " + className1
        + " WHERE name = 'Inactive') ;").close();

    final ResultSet result = database.query("sql", "seLECT FROM " + className2);
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Object val = row.getProperty("processingType");
      assertThat(val).isNotNull();
      assertThat(val instanceof Identifiable).isTrue();
    }
    result.close();
  }

  @Test
  void listConversion() {
    final String className1 = "testLISTConversion1";
    final String className2 = "testLISTConversion2";

    database.command("sql", "create document type " + className1).close();

    database.command("sql", "create document type " + className2 + ";").close();
    database.command("sql", "CREATE PROPERTY " + className2 + ".sub LIST;").close();

    database.command("sql", "INSERT INTO " + className2 + " SET name='Active', sub = [{'name':'foo'}];").close();

    final ResultSet result = database.query("sql", "seLECT FROM " + className2);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Object list = row.getProperty("sub");
      assertThat(list).isNotNull();
      assertThat(list instanceof List).isTrue();
      assertThat(((List) list).size()).isEqualTo(1);

      final Object o = ((List) list).get(0);
      assertThat(o instanceof Map).isTrue();
      assertThat(((Map) o).get("name")).isEqualTo("foo");
    }
    result.close();
  }

  @Test
  void listConversion2() {
    final String className1 = "testLISTConversion21";
    final String className2 = "testLISTConversion22";

    database.command("sql", "create document type " + className1).close();

    database.command("sql", "create document type " + className2 + ";").close();
    database.command("sql", "CREATE PROPERTY " + className2 + ".sub LIST;").close();

    database.command("sql", "INSERT INTO " + className2 + " (name, sub) values ('Active', [{'name':'foo'}]);").close();

    final ResultSet result = database.query("sql", "seLECT FROM " + className2);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Object list = row.getProperty("sub");
      assertThat(list).isNotNull();
      assertThat(list instanceof List).isTrue();
      assertThat(((List) list).size()).isEqualTo(1);

      final Object o = ((List) list).get(0);
      assertThat(o instanceof Map).isTrue();
      assertThat(((Map) o).get("name")).isEqualTo("foo");
    }
    result.close();
  }

  @Test
  void insertReturn() {
    final String className = "testInsertReturn";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql", "insert into " + className + " set name = 'name1' RETURN 'OK' as result");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("result")).isEqualTo("OK");
    }
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "select from " + className);
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void nestedInsert() {
    final String className = "testNestedInsert";
    database.getSchema().createDocumentType(className);

    ResultSet result = database.command("sql",
        "insert into " + className + " set name = 'parent', children = (INSERT INTO " + className + " SET name = 'child')");

    result.close();

    result = database.query("sql", "seLECT FROM " + className);

    for (int i = 0; i < 2; i++) {
      final Result item = result.next();
      if (item.<String>getProperty("name").equals("parent")) {
        assertThat(item.getProperty("children") instanceof Collection).isTrue();
        assertThat(((Collection) item.getProperty("children")).size()).isEqualTo(1);
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void linkMapWithSubqueries() {
    final String className = "testLinkMapWithSubqueries";
    final String itemclassName = "testLinkMapWithSubqueriesTheItem";

    database.command("sql", "create document type " + className);
    database.command("sql", "create document type " + itemclassName);
    database.command("sql", "CREATE PROPERTY " + className + ".mymap MAP");

    database.command("sql", "INSERT INTO " + itemclassName + " (name) VALUES ('test')");
    database.command("sql",
        "INSERT INTO " + className + " (mymap) VALUES ({'A-1': (SELECT FROM " + itemclassName + " WHERE name = 'test')})");

    final ResultSet result = database.query("sql", "seLECT FROM " + className);

    final Result item = result.next();
    final Map theMap = item.getProperty("mymap");
    assertThat(theMap.size()).isEqualTo(1);
    assertThat(theMap.get("A-1")).isNotNull();

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void quotedCharactersInJson() {
    final String className = "testQuotedCharactersInJson";

    database.command("sql", "create document type " + className);

    database.command("sql", "INSERT INTO " + className + " CONTENT { name: \"jack\", memo: \"this is a \\n multi line text\" }");

    final ResultSet result = database.query("sql", "seLECT FROM " + className);

    final Result item = result.next();
    final String memo = item.getProperty("memo");
    assertThat(memo).isEqualTo("this is a \n multi line text");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void insertEdgeMustFail() {
    final String className = "testInsertEdge";
    database.getSchema().createEdgeType(className);
    assertThatThrownBy(() -> database.command("sql", "insert into " + className + " set `@out` = #1:10, `@in` = #1:11")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void insertJsonNewLines() {
    database.getSchema().createDocumentType("doc");
    final ResultSet result = database.command("sql", "INSERT INTO doc CONTENT {\n" + //
        "\"head\" : {\n" + //
        "  \"vars\" : [ \"item\", \"itemLabel\" ]\n" + //
        "},\n" + //
        "\"results\" : {\n" + //
        "  \"bindings\" : [ {\n" + //
        "    \"item\" : {\n" + //
        "          \"type\" : \"uri\",\n" + //
        "              \"value\" : \"http://www.wikidata.org/entity/Q113997665\"\n" + //
        "        },\n" + //
        "        \"itemLabel\" : {\n" + //
        "          \"xml:lang\" : \"en\",\n" + //
        "              \"type\" : \"literal\",\n" + //
        "              \"value\" : \"ArcadeDB\"\n" + //
        "        }\n" + //
        "      }, {\n" + //
        "        \"item\" : {\n" + //
        "          \"type\" : \"uri\",\n" + //
        "              \"value\" : \"http://www.wikidata.org/entity/Q808716\"\n" + //
        "        },\n" + //
        "        \"itemLabel\" : {\n" + //
        "          \"xml:lang\" : \"en\",\n" + //
        "              \"type\" : \"literal\",\n" + //
        "              \"value\" : \"OrientDB\"\n" + //
        "        }\n" + //
        "      } ]\n" + //
        "    }\n" + //
        "}");

    assertThat(result.hasNext()).isTrue();
    final Result res = result.next();
    assertThat(res.hasProperty("head")).isTrue();
    assertThat(res.hasProperty("results")).isTrue();
  }

  @Test
  void insertEncoding() {
    database.getSchema().createDocumentType("RegInfoDoc");
    final ResultSet result = database.command("sql",
        "insert into RegInfoDoc set payload = \"(Pn/m)*1000kg/kW, with \\\"Pn\\\" being the\\n\\np  and \\\"m\\\" (kg)\"");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("payload")).isEqualTo("(Pn/m)*1000kg/kW, with \"Pn\" being the\n\np  and \"m\" (kg)");
  }

  @Test
  void insertFromSelect() {
    database.command("sqlscript",
        """
        CREATE DOCUMENT TYPE src;
        CREATE DOCUMENT TYPE dst;
        """
    );

    final ResultSet result = database.command("sqlscript",
        """
        INSERT INTO src SET a = 1;
        INSERT INTO src SET a = 2;
        INSERT INTO src SET a = 3;
        INSERT INTO dst FROM SELECT a FROM src;\
        """
    );
    int i = 0;
    for (; result.hasNext(); i++)
      result.next();

    assertThat(i).isEqualTo(3);
  }

  @Test
  void insertIntoBucketFromSubquery() {
    // Test case for issue #1961: INSERT INTO BUCKET FROM (subquery) silently fails
    database.command("sql", "CREATE DOCUMENT TYPE src");
    database.command("sql", "INSERT INTO src SET name = 'Hi'");

    database.command("sql", "CREATE DOCUMENT TYPE doc BUCKET doc");

    // This should insert the record into the bucket
    ResultSet result = database.command("sql", "INSERT INTO BUCKET:doc FROM (SELECT name FROM src)");

    // Should return 1 inserted record
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("name")).isEqualTo("Hi");
    assertThat(result.hasNext()).isFalse();
    result.close();

    // Verify the record was actually inserted into the bucket
    result = database.query("sql", "SELECT FROM BUCKET:doc");
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("name")).isEqualTo("Hi");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void insertIntoBucketFromSubqueryWithMultipleBuckets() {
    // Test case for issue #1961 scenario 3: INSERT INTO BUCKET:doc FROM (SELECT name FROM BUCKET:src)
    // where doc type has multiple buckets
    database.command("sql", "CREATE DOCUMENT TYPE doc BUCKET doc, src");
    database.command("sql", "INSERT INTO BUCKET:src SET name = 'Hi'");

    // This should insert the record from src bucket into doc bucket
    ResultSet result = database.command("sql", "INSERT INTO BUCKET:doc FROM (SELECT name FROM BUCKET:src)");

    // Should return 1 inserted record
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("name")).isEqualTo("Hi");
    assertThat(result.hasNext()).isFalse();
    result.close();

    // Verify the record was actually inserted into the doc bucket
    result = database.query("sql", "SELECT FROM BUCKET:doc");
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("name")).isEqualTo("Hi");
    assertThat(result.hasNext()).isFalse();
    result.close();

    // Verify the source record still exists in src bucket
    result = database.query("sql", "SELECT FROM BUCKET:src");
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("name")).isEqualTo("Hi");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void insertWithLanguageKeywordAsPropertyName() {
    // Test case for issue #3212: INSERT with 'language' keyword as property name
    final String className = "testInsertLanguage";
    database.getSchema().createDocumentType(className);

    // Test simple insert with language property
    ResultSet result = database.command("sql", "INSERT INTO " + className + " SET hash = 'abc', language = 'en'");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("hash")).isEqualTo("abc");
    assertThat(item.<String>getProperty("language")).isEqualTo("en");
    assertThat(result.hasNext()).isFalse();
    result.close();

    // Test with parameters (as in the original issue)
    final Map<String, Object> params = new HashMap<>();
    params.put("hash", "def");
    params.put("language", "fr");
    result = database.command("sql", "INSERT INTO " + className + " SET hash = :hash, language = :language", params);
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("hash")).isEqualTo("def");
    assertThat(item.<String>getProperty("language")).isEqualTo("fr");
    assertThat(result.hasNext()).isFalse();
    result.close();

    // Verify the two records were created
    result = database.query("sql", "SELECT FROM " + className);
    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
    result.close();
  }

  @Test
  void insertWithLanguageKeywordInScript() {
    // Test case for issue #3212: Complete example with LET and RETURN
    final String className = "testInsertLanguageScript";
    database.getSchema().createDocumentType(className);

    final Map<String, Object> params = new HashMap<>();
    params.put("hash", "ghi");
    params.put("language", "es");
    ResultSet result = database.command("sqlscript",
        "LET $newRecord = INSERT INTO " + className + " SET hash = :hash, language = :language RETURN @rid;\n" +
        "RETURN { \"created\": true, \"rid\": $newRecord['@rid'] };",
        params);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();

    // The result is wrapped in a "value" property for sqlscript RETURN statements
    Map<String, Object> value = item.getProperty("value");
    assertThat(value).isNotNull();
    assertThat((Boolean) value.get("created")).isTrue();
    assertThat(value.get("rid")).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();

    // Verify the record was created
    result = database.query("sql", "SELECT FROM " + className);
    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
    result.close();
  }

  @Test
  void insertWithLanguageKeywordExactIssueQuery() {
    // Test case for issue #3212: Exact query from the GitHub issue
    database.getSchema().createDocumentType("DOCUMENT");

    // This is the exact query that was failing in the issue
    final Map<String, Object> params = new HashMap<>();
    params.put("hash", "test123");
    params.put("language", "en-US");

    ResultSet result = database.command("sqlscript",
        """
        LET $newRecord = INSERT INTO `DOCUMENT` SET hash = :hash, language = :language RETURN @rid;
        RETURN { "created": true, "rid": $newRecord['@rid'] };\
        """,
        params);

    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();

    Map<String, Object> value = item.getProperty("value");
    assertThat(value).isNotNull();
    assertThat((Boolean) value.get("created")).isTrue();
    assertThat(value.get("rid")).isNotNull();
    result.close();

    // Verify the record was created with correct properties
    result = database.query("sql", "SELECT FROM DOCUMENT");
    assertThat(result.hasNext()).isTrue();
    Result doc = result.next();
    assertThat(doc.<String>getProperty("hash")).isEqualTo("test123");
    assertThat(doc.<String>getProperty("language")).isEqualTo("en-US");
    result.close();
  }
}
