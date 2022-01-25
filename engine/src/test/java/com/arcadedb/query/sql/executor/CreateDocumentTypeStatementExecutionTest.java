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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CreateDocumentTypeStatementExecutionTest {
  @Test
  public void testPlain() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      String className = "testPlain";
      ResultSet result = db.command("sql", "create document type " + className);
      Schema schema = db.getSchema();
      DocumentType clazz = schema.getType(className);
      Assertions.assertNotNull(clazz);
      result.close();
    });
  }

  @Test
  public void testClusters() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      String className = "testClusters";
      ResultSet result = db.command("sql", "create document type " + className + " buckets 32");
      Schema schema = db.getSchema();
      DocumentType clazz = schema.getType(className);
      Assertions.assertNotNull(clazz);
      Assertions.assertEquals(32, clazz.getBuckets(false).size());
      result.close();
    });
  }

  @Test
  public void testIfNotExists() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      String className = "testIfNotExists";
      ResultSet result = db.command("sql", "create document type " + className + " if not exists");
      Schema schema = db.getSchema();
      DocumentType clazz = schema.getType(className);
      Assertions.assertNotNull(clazz);
      result.close();

      result = db.command("sql", "create document type " + className + " if not exists");
      clazz = schema.getType(className);
      Assertions.assertNotNull(clazz);
      result.close();
    });
  }
}
