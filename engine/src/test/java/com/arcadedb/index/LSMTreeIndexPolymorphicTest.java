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
 */
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;

import java.util.*;

public class LSMTreeIndexPolymorphicTest extends TestHelper {

  //@Test
  public void testDocumentAfterCreation() {
    DocumentType typeRoot = database.getSchema().getOrCreateDocumentType("TestRoot");
    typeRoot.getOrCreateProperty("name", String.class);
    typeRoot.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    database.command("sql", "delete from TestRoot");

    DocumentType typeChild = database.getSchema().getOrCreateDocumentType("TestChild");
    typeChild.setParentTypes(Arrays.asList(typeRoot));

    MutableDocument doc = database.newDocument("TestChild");

    database.transaction(() -> {
      doc.set("name", "Document Name");
      Assertions.assertEquals("Document Name", doc.get("name"));
      doc.save();
    });

    Assertions.assertEquals("Document Name", doc.get("name"));
    try (ResultSet rs = database.query("sql", "select from TestChild where name = :name", Map.of("arg0", "Test2", "name", "Document Name"))) {
      Assertions.assertTrue(rs.hasNext());
      Document docRetrieved = rs.next().getElement().orElse(null);
      Assertions.assertEquals("Document Name", docRetrieved.get("name"));
      Assertions.assertFalse(rs.hasNext());
    }
  }
}
