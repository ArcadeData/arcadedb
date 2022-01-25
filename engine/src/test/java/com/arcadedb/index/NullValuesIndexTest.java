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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NullValuesIndexTest extends TestHelper {
  private static final int    TOT       = 10;
  private static final String TYPE_NAME = "V";
  private static final int    PAGE_SIZE = 20000;

  @Test
  public void testNullStrategyError() {
    Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

    final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    final Index indexes = database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, PAGE_SIZE);
    final Index indexes2 = database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "name" }, PAGE_SIZE, LSMTreeIndexAbstract.NULL_STRATEGY.ERROR, null);

    try {
      database.transaction(() -> {
        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newDocument(TYPE_NAME);
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner");
          v.save();
        }

        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", TOT);
        v.save();

        database.commit();
        database.begin();

        for (Index index : ((TypeIndex) indexes).getIndexesOnBuckets()) {
          Assertions.assertTrue(((IndexInternal) index).getStats().get("pages") > 1);
        }
      });
      Assertions.fail();
    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IllegalArgumentException);
      Assertions.assertTrue(e.getCause().getMessage().startsWith("Indexed key V[name] cannot be NULL"));
    }
  }

  @Test
  public void testNullStrategySkip() {
    Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

    final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    final Index indexes = database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, PAGE_SIZE);
    final Index indexes2 = database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, new String[] { "name" }, PAGE_SIZE, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, null);

    database.transaction(() -> {
      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");
        v.save();
      }

      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", TOT);
      v.save();

      database.commit();
      database.begin();
    });

    database.transaction(() -> Assertions.assertEquals(database.countType(TYPE_NAME, true), TOT + 1));

    database.close();
    database = factory.open();

    // TRY AGAIN WITH A RE-OPEN DATABASE
    database.transaction(() -> {
      Assertions.assertTrue(database.getSchema().existsType(TYPE_NAME));

      for (int i = TOT + 2; i < TOT + TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay" + i);
        v.set("surname", "Miner");
        v.save();
      }

      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", TOT + TOT + 1);
      v.save();

      database.commit();
      database.begin();
    });

    database.transaction(() -> Assertions.assertEquals(database.countType(TYPE_NAME, true), TOT + TOT));
  }

}
