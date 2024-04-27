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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class DropIndexTest extends TestHelper {
  private static final int    TOT        = 10;
  private static final String TYPE_NAME  = "V";
  private static final String TYPE_NAME2 = "V2";
  private static final String TYPE_NAME3 = "V3";
  private static final int    PAGE_SIZE  = 20000;

  @Test
  public void testDropAndRecreate() {
    Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
    final DocumentType type2 = database.getSchema().buildDocumentType().withName(TYPE_NAME2).withTotalBuckets(3).create();
    type2.addSuperType(type);

    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);

    final Index typeIndex = database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, PAGE_SIZE);
    final Index typeIndex2 = database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "name" }, PAGE_SIZE,
            LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, null);

    database.transaction(() -> {
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

      Assertions.assertEquals(TOT + 1, database.countType(TYPE_NAME, true));
      Assertions.assertEquals(TOT, database.countType(TYPE_NAME2, false));
      Assertions.assertEquals(1, database.countType(TYPE_NAME, false));

      database.begin();

      final Index[] subIndexes = ((TypeIndex) typeIndex).getIndexesOnBuckets();

      database.getSchema().dropIndex(typeIndex.getName());

      for (final Index idx : subIndexes)
        try {
          database.getSchema().getIndexByName(idx.getName());
          Assertions.fail("Found removed index " + idx.getName());
        } catch (final SchemaException e) {
        }

      for (final Index subIndex : subIndexes)
        try {
          database.getSchema().getFileById(subIndex.getAssociatedBucketId());
          database.getSchema().getFileById(((IndexInternal) subIndex).getFileId());
          Assertions.fail("Found removed file " + ((IndexInternal) subIndex).getFileId());
        } catch (final SchemaException e) {
        }

      final Index typeIndex3 = database.getSchema()
          .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, PAGE_SIZE);

      Assertions.assertEquals(TOT + 1, database.countType(TYPE_NAME, true));
      Assertions.assertEquals(TOT, database.countType(TYPE_NAME2, false));
      Assertions.assertEquals(1, database.countType(TYPE_NAME, false));

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v2 = database.newDocument(TYPE_NAME2);
        v2.set("id", TOT * 2 + i);
        v2.set("name", "Jay2");
        v2.set("surname", "Miner2");
        v2.save();
      }

      final MutableDocument v3 = database.newDocument(TYPE_NAME);
      v3.set("id", TOT * 3 + 1);
      v3.save();

      Assertions.assertEquals(TOT * 2 + 2, database.countType(TYPE_NAME, true));
      Assertions.assertEquals(TOT * 2, database.countType(TYPE_NAME2, false));
      Assertions.assertEquals(2, database.countType(TYPE_NAME, false));
    }, false, 0);
  }

  @Test
  public void testDropAndRecreateTypeWithIndex() {
    Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
    final DocumentType type2 = database.getSchema().buildDocumentType().withName(TYPE_NAME2).withTotalBuckets(3)
        .withSuperType(type.getName()).create();

    Assertions.assertEquals(type.getName(), type2.getSuperTypes().getFirst().getName());

    final DocumentType type3 = database.getSchema().buildDocumentType().withName(TYPE_NAME3).withTotalBuckets(3)
        .withSuperType(type2.getName()).create();

    Assertions.assertEquals(type.getName(), type2.getSuperTypes().getFirst().getName());

    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);

    final Index typeIndex = database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, PAGE_SIZE);
    final Index typeIndex2 = database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "name" }, PAGE_SIZE,
            LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, null);

    type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy());
    type2.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy());

    database.transaction(() -> {
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
      for (final Bucket b : buckets) {
        try {
          database.getSchema().getBucketById(b.getFileId());
          Assertions.fail();
        } catch (final SchemaException e) {
          // EXPECTED
        }

        try {
          database.getSchema().getBucketByName(b.getName());
          Assertions.fail();
        } catch (final SchemaException e) {
          // EXPECTED
        }

        try {
          database.getSchema().getFileById(b.getFileId());
          Assertions.fail();
        } catch (final SchemaException e) {
          // EXPECTED
        }
      }

      // CHECK ALL THE INDEXES ARE NOT REMOVED
      database.getSchema().getIndexByName(typeIndex.getName());
      database.getSchema().getIndexByName(typeIndex2.getName());

      // CHECK TYPE HAS BEEN REMOVED FROM INHERITANCE
      for (final DocumentType parent : type2.getSuperTypes())
        Assertions.assertFalse(parent.getSubTypes().contains(type2));

      for (final DocumentType sub : type2.getSubTypes())
        Assertions.assertFalse(sub.getSuperTypes().contains(type2));

      // CHECK INHERITANCE CHAIN IS CONSISTENT
      for (final DocumentType parent : type2.getSuperTypes())
        Assertions.assertTrue(parent.getSubTypes().contains(type2.getSubTypes().getFirst()));

      Assertions.assertEquals(1, database.countType(TYPE_NAME, true));

      final DocumentType newType = database.getSchema().getOrCreateDocumentType(TYPE_NAME2);

      Assertions.assertEquals(1, database.countType(TYPE_NAME, true));
      Assertions.assertEquals(0, database.countType(TYPE_NAME2, true));

      newType.addSuperType(TYPE_NAME);

      // CHECK INHERITANCE CHAIN IS CONSISTENT AGAIN
      for (final DocumentType parent : newType.getSuperTypes())
        Assertions.assertTrue(parent.getSubTypes().contains(newType));

      for (final DocumentType sub : newType.getSubTypes())
        Assertions.assertTrue(sub.getSuperTypes().contains(newType));

      Assertions.assertEquals(1, database.countType(TYPE_NAME, true));
      Assertions.assertEquals(0, database.countType(TYPE_NAME2, true));

      database.begin();

      database.getSchema().dropIndex(typeIndex.getName());
      final Index typeIndex3 = database.getSchema()
          .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, PAGE_SIZE);

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

      type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy());

      database.getSchema().dropIndex(typeIndex3.getName());

    }, false, 0);
  }
}
