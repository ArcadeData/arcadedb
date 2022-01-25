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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.parser.IndexIdentifier;
import com.arcadedb.query.sql.parser.IndexName;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class CountFromIndexStepTest {

  private static final String PROPERTY_NAME  = "testPropertyName";
  private static final String PROPERTY_VALUE = "testPropertyValue";
  private static final String ALIAS          = "size";
  private static       String indexName;

  private final IndexIdentifier.Type identifierType;

  public CountFromIndexStepTest() {
    this.identifierType = IndexIdentifier.Type.INDEX;
  }

  public static Iterable<Object[]> types() {
    return Arrays.asList(new Object[][] { { IndexIdentifier.Type.INDEX }, { IndexIdentifier.Type.VALUES }, { IndexIdentifier.Type.VALUESASC },
        { IndexIdentifier.Type.VALUESDESC }, });
  }

  @Test
  public void shouldCountRecordsOfIndex() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      DocumentType clazz = TestHelper.createRandomType(db);
      clazz.createProperty(PROPERTY_NAME, Type.STRING);
      String className = clazz.getName();
      indexName = className + "[" + PROPERTY_NAME + "]";
      clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, PROPERTY_NAME);

      for (int i = 0; i < 20; i++) {
        MutableDocument document = db.newDocument(className);
        document.set(PROPERTY_NAME, PROPERTY_VALUE);
        document.save();
      }

      className = TestHelper.createRandomType(db).getName();
      IndexName name = new IndexName(-1);
      name.setValue(indexName);
      IndexIdentifier identifier = new IndexIdentifier(-1);
      identifier.setIndexName(name);
      identifier.setIndexNameString(name.getValue());
      identifier.setType(identifierType);

      BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      CountFromIndexStep step = new CountFromIndexStep(identifier, ALIAS, context, false);

      ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(20, (long) result.next().getProperty(ALIAS));
      Assertions.assertFalse(result.hasNext());
    });
  }
}
