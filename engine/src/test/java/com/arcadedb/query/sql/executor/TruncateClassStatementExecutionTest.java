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
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class TruncateClassStatementExecutionTest extends TestHelper {

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClass() {
    database.begin();

    final Schema schema = database.getSchema();
    final DocumentType testClass = getOrcreateDocumentType(schema);

    final Index index = getOrCreateIndex(testClass);

    database.command("sql", "truncate type test_class");

    database.newDocument(testClass.getName()).set("name", "x").set("data", Arrays.asList(1, 2)).save();
    database.newDocument(testClass.getName()).set("name", "y").set("data", Arrays.asList(3, 0)).save();

    database.command("sql", "truncate type test_class");

    database.newDocument(testClass.getName()).set("name", "x").set("data", Arrays.asList(5, 6, 7)).save();
    database.newDocument(testClass.getName()).set("name", "y").set("data", Arrays.asList(8, 9, -1)).save();

    final ResultSet result = database.query("sql", "SElect from test_class");
    //    Assertions.assertThat(2).isEqualTo(result.size());

    final Set<Integer> set = new HashSet<Integer>();
    while (result.hasNext()) {
      set.addAll(result.next().getProperty("data"));
    }
    result.close();
    assertThat(set.containsAll(Arrays.asList(5, 6, 7, 8, 9, -1))).isTrue();

    schema.dropType("test_class");
    database.commit();
  }


  @Test
  public void testTruncateVertexClassSubclasses() {
    database.begin();
    database.command("sql", "create document type TestTruncateVertexClassSuperclass");
    database.command("sql", "create document type TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass");

    database.command("sql", "insert into TestTruncateVertexClassSuperclass set name = 'foo'");
    database.command("sql", "insert into TestTruncateVertexClassSubclass set name = 'bar'");

    ResultSet result = database.query("sql", "SElect from TestTruncateVertexClassSuperclass");
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      result.next();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();

    database.command("sql", "truncate type TestTruncateVertexClassSuperclass ");
    result = database.query("sql", "SElect from TestTruncateVertexClassSubclass");
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();

    database.command("sql", "truncate type TestTruncateVertexClassSuperclass polymorphic");
    result = database.query("sql", "SElect from TestTruncateVertexClassSubclass");
    assertThat(result.hasNext()).isFalse();
    result.close();
    database.commit();
  }

  @Test
  public void testTruncateVertexClassSubclassesWithIndex() {
    database.begin();
    database.command("sql", "create document type TestTruncateVertexClassSuperclassWithIndex");
    database.command("sql", "create property TestTruncateVertexClassSuperclassWithIndex.name STRING");
    database.command("sql", "create index on TestTruncateVertexClassSuperclassWithIndex (name) NOTUNIQUE");

    database.command("sql", "create document type TestTruncateVertexClassSubclassWithIndex extends TestTruncateVertexClassSuperclassWithIndex");

    database.command("sql", "insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'");
    database.command("sql", "insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'");

    database.command("sql", "truncate type TestTruncateVertexClassSubclassWithIndex");
    database.command("sql", "truncate type TestTruncateVertexClassSuperclassWithIndex polymorphic");
    database.commit();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClassWithCommandCache() {
    database.begin();

    final Schema schema = database.getSchema();
    final DocumentType testClass = getOrcreateDocumentType(schema);

    database.command("sql", "truncate type test_class");

    database.newDocument(testClass.getName()).set("name", "x").set("data", Arrays.asList(1, 2)).save();
    database.newDocument(testClass.getName()).set("name", "y").set("data", Arrays.asList(3, 0)).save();

    ResultSet result = database.query("sql", "SElect from test_class");
    assertThat(toList(result).size()).isEqualTo(2);

    result.close();
    database.command("sql", "truncate type test_class");

    result = database.query("sql", "SElect from test_class");
    assertThat(toList(result).size()).isEqualTo(0);
    result.close();

    schema.dropType("test_class");

    database.commit();
  }

  private List<Result> toList(final ResultSet input) {
    final List<Result> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }

  private Index getOrCreateIndex(final DocumentType testClass) {
    if (database.getSchema().existsIndex("test_class_by_data"))
      return database.getSchema().getIndexByName("test_class_by_data");

    testClass.createProperty("data", Type.LIST);
    return testClass.createTypeIndex(Schema.IndexType.LSM_TREE, true, "data");
  }

  private DocumentType getOrcreateDocumentType(final Schema schema) {
    final DocumentType testClass;
    if (schema.existsType("test_class")) {
      testClass = schema.getType("test_class");
    } else {
      testClass = schema.createDocumentType("test_class");
    }
    return testClass;
  }
}
