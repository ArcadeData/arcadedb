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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DropTypeTest extends TestHelper {
  private static final int    TOT        = 10;
  private static final String TYPE_NAME  = "V";
  private static final String TYPE_NAME2 = "V2";
  private static final String TYPE_NAME3 = "V3";

  @Test
  public void testDropAndRecreateType() {
    database.transaction(() -> {
      Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);

      final DocumentType type2 = database.getSchema().createDocumentType(TYPE_NAME2, 3);
      type2.addSuperType(type);

      final DocumentType type3 = database.getSchema().createDocumentType(TYPE_NAME3, 3);
      type3.addSuperType(type2);

      type.createProperty("id", Integer.class);
      type.createProperty("name", String.class);

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME2);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");
        v.save();
      }

      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", TOT);
      v.save();

      database.commit();

      final List<Bucket> buckets = type2.getBuckets(false);

      database.getSchema().dropType(TYPE_NAME2);

      // CHECK ALL THE BUCKETS ARE REMOVED
      for (Bucket b : buckets) {
        try {
          database.getSchema().getBucketById(b.getId());
          Assertions.fail();
        } catch (SchemaException e) {
        }

        try {
          database.getSchema().getBucketByName(b.getName());
          Assertions.fail();
        } catch (SchemaException e) {
        }

        try {
          database.getSchema().getFileById(b.getId());
          Assertions.fail();
        } catch (SchemaException e) {
        }
      }

      // CHECK TYPE HAS BEEN REMOVED FROM INHERITANCE
      for (DocumentType parent : type2.getSuperTypes())
        Assertions.assertFalse(parent.getSubTypes().contains(type2));

      for (DocumentType sub : type2.getSubTypes())
        Assertions.assertFalse(sub.getSuperTypes().contains(type2));

      // CHECK INHERITANCE CHAIN IS CONSISTENT
      for (DocumentType parent : type2.getSuperTypes())
        Assertions.assertTrue(parent.getSubTypes().contains(type2.getSubTypes().get(0)));

      for (DocumentType sub : type2.getSubTypes())
        Assertions.assertTrue(sub.getSuperTypes().contains(type2.getSuperTypes().get(0)));

      Assertions.assertEquals(1, database.countType(TYPE_NAME, true));

      DocumentType newType = database.getSchema().getOrCreateDocumentType(TYPE_NAME2);
      Assertions.assertEquals(1, database.countType(TYPE_NAME, true));
      Assertions.assertEquals(0, database.countType(TYPE_NAME2, true));
      Assertions.assertEquals(0, database.countType(TYPE_NAME2, false));

      newType.addSuperType(TYPE_NAME);

      // CHECK INHERITANCE CHAIN IS CONSISTENT AGAIN
      for (DocumentType parent : newType.getSuperTypes())
        Assertions.assertTrue(parent.getSubTypes().contains(newType));

      for (DocumentType sub : newType.getSubTypes())
        Assertions.assertTrue(sub.getSuperTypes().contains(newType));

      Assertions.assertEquals(1, database.countType(TYPE_NAME, true));
      Assertions.assertEquals(0, database.countType(TYPE_NAME2, true));
      Assertions.assertEquals(0, database.countType(TYPE_NAME2, false));

      database.begin();

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v2 = database.newDocument(TYPE_NAME2);
        v2.set("id", TOT * 2 + i);
        v2.set("name", "Jay");
        v2.set("surname", "Miner");
        v2.save();
      }

      final MutableDocument v3 = database.newDocument(TYPE_NAME);
      v3.set("id", TOT);
      v3.save();

      Assertions.assertEquals(TOT + 2, database.countType(TYPE_NAME, true));
      Assertions.assertEquals(TOT, database.countType(TYPE_NAME2, false));
      Assertions.assertEquals(2, database.countType(TYPE_NAME, false));
    });
  }
}
