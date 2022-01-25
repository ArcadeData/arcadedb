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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

public class LSMTreeIndexPolymorphicTest extends TestHelper {

  private DocumentType typeChild;
  private DocumentType typeRoot;

  @Test
  public void testPolymorphic() {
    populate(Schema.INDEX_TYPE.LSM_TREE);

    try {
      MutableDocument docChildDuplicated = database.newDocument("TestChild");
      database.transaction(() -> {
        docChildDuplicated.set("name", "Root");
        Assertions.assertEquals("Root", docChildDuplicated.get("name"));
        docChildDuplicated.save();
      }, true, 0);

      Assertions.fail("Duplicated shouldn't be allowed by unique index on sub type");

    } catch (DuplicatedKeyException e) {
      // EXPECTED
    }

    checkQueries();
  }

  @Test
  public void testPolymorphicFullText() {
    populate(Schema.INDEX_TYPE.FULL_TEXT);
    checkQueries();
  }

  // https://github.com/ArcadeData/arcadedb/issues/152
  @Test
  public void testDocumentAfterCreation2() {
    DocumentType typeRoot2 = database.getSchema().getOrCreateDocumentType("TestRoot2");
    typeRoot2.getOrCreateProperty("name", String.class);
    typeRoot2.getOrCreateProperty("parent", Type.LINK);
    typeRoot2.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name", "parent");
    database.command("sql", "delete from TestRoot2");
    database.begin();
    DocumentType testChild2 = database.getSchema().getOrCreateDocumentType("TestChild2");
    testChild2.setSuperTypes(Arrays.asList(typeRoot2));
    MutableDocument doc = database.newDocument("TestChild2");
    doc.set("name", "Document Name");
    Assertions.assertEquals("Document Name", doc.get("name"));
    doc.save();
    Assertions.assertEquals("Document Name", doc.get("name"));
    try (ResultSet rs = database.query("sql", "select from TestChild2 where name = :name", Map.of("arg0", "Test2", "name", "Document Name"))) {
      Assertions.assertTrue(rs.hasNext());
      Document docRetrieved = rs.next().getElement().orElse(null);
      Assertions.assertEquals("Document Name", docRetrieved.get("name"));
      Assertions.assertFalse(rs.hasNext());
    }
    database.commit();
  }

  // https://github.com/ArcadeData/arcadedb/issues/152
  @Test
  public void testDocumentAfterCreation2AutoTx() {
    DocumentType typeRoot2 = database.getSchema().getOrCreateDocumentType("TestRoot2");
    typeRoot2.getOrCreateProperty("name", String.class);
    typeRoot2.getOrCreateProperty("parent", Type.LINK);
    typeRoot2.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name", "parent");
    database.command("sql", "delete from TestRoot2");
    DocumentType typeChild2 = database.getSchema().getOrCreateDocumentType("TestChild2");
    typeChild2.setSuperTypes(Arrays.asList(typeRoot2));

    database.setAutoTransaction(true);
    MutableDocument doc = database.newDocument("TestChild2");
    doc.set("name", "Document Name");
    Assertions.assertEquals("Document Name", doc.get("name"));
    doc.save();
    Assertions.assertEquals("Document Name", doc.get("name"));
    try (ResultSet rs = database.query("sql", "select from TestChild2 where name = :name", Map.of("arg0", "Test2", "name", "Document Name"))) {
      Assertions.assertTrue(rs.hasNext());  //<<<<<<----------FAILING HERE
      Document docRetrieved = rs.next().getElement().orElse(null);
      Assertions.assertEquals("Document Name", docRetrieved.get("name"));
      Assertions.assertFalse(rs.hasNext());
    }
  }

  private void populate(Schema.INDEX_TYPE indexType) {
    typeRoot = database.getSchema().getOrCreateDocumentType("TestRoot");
    typeRoot.getOrCreateProperty("name", String.class);
    typeRoot.getOrCreateTypeIndex(indexType, true, "name");
    database.command("sql", "delete from TestRoot");

    typeChild = database.getSchema().getOrCreateDocumentType("TestChild");
    typeChild.setSuperTypes(Arrays.asList(typeRoot));

    MutableDocument docRoot = database.newDocument("TestRoot");
    database.transaction(() -> {
      docRoot.set("name", "Root");
      Assertions.assertEquals("Root", docRoot.get("name"));
      docRoot.save();
    });
    Assertions.assertEquals("Root", docRoot.get("name"));

    MutableDocument docChild = database.newDocument("TestChild");
    database.transaction(() -> {
      docChild.set("name", "Child");
      Assertions.assertEquals("Child", docChild.get("name"));
      docChild.save();
    });
    Assertions.assertEquals("Child", docChild.get("name"));
  }

  private void checkQueries() {
    try (ResultSet rs = database.query("sql", "select from TestRoot where name <> :name", Map.of("arg0", "Test2", "name", "Nonsense"))) {
      Assertions.assertTrue(rs.hasNext());
      Document doc1Retrieved = rs.next().getElement().orElse(null);
      Assertions.assertTrue(rs.hasNext());
      Document doc2Retrieved = rs.next().getElement().orElse(null);

      if (doc1Retrieved.getTypeName().equals("TestRoot"))
        Assertions.assertEquals("Root", doc1Retrieved.get("name"));
      else if (doc2Retrieved.getTypeName().equals("TestChild"))
        Assertions.assertEquals("Child", doc2Retrieved.get("name"));
      else
        Assertions.fail();

      Assertions.assertFalse(rs.hasNext());
    }

    try (ResultSet rs = database.query("sql", "select from TestChild where name = :name", Map.of("arg0", "Test2", "name", "Child"))) {
      Assertions.assertTrue(rs.hasNext());
      Document doc1Retrieved = rs.next().getElement().orElse(null);
      Assertions.assertEquals("Child", doc1Retrieved.get("name"));
      Assertions.assertFalse(rs.hasNext());
    }

    typeChild.removeSuperType(typeRoot);

    try (ResultSet rs = database.query("sql", "select from TestChild where name = :name", Map.of("arg0", "Test2", "name", "Child"))) {
      Assertions.assertTrue(rs.hasNext());
      Document doc1Retrieved = rs.next().getElement().orElse(null);
      Assertions.assertEquals("Child", doc1Retrieved.get("name"));
      Assertions.assertFalse(rs.hasNext());
    }

    try (ResultSet rs = database.query("sql", "select from TestRoot where name <> :name", Map.of("arg0", "Test2", "name", "Nonsense"))) {
      Assertions.assertTrue(rs.hasNext());
      Document doc1Retrieved = rs.next().getElement().orElse(null);
      Assertions.assertEquals("Root", doc1Retrieved.get("name"));
      Assertions.assertFalse(rs.hasNext());
    }
  }
}
