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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CreateDocumentTypeStatementExecutionTest {
  @Test
  public void testPlain() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = "testPlain";
      final ResultSet result = db.command("sql", "create document type " + className);
      final Schema schema = db.getSchema();
      final DocumentType clazz = schema.getType(className);
      assertThat(clazz).isNotNull();
      result.close();
    });
  }

  @Test
  public void testClusters() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = "testClusters";
      final ResultSet result = db.command("sql", "create document type " + className + " buckets 32");
      final Schema schema = db.getSchema();
      final DocumentType clazz = schema.getType(className);
      assertThat(clazz).isNotNull();
      assertThat(clazz.getBuckets(false)).hasSize(32);
      result.close();
    });
  }

  @Test
  public void testIfNotExists() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = "testIfNotExists";
      ResultSet result = db.command("sql", "create document type " + className + " if not exists");
      final Schema schema = db.getSchema();
      DocumentType clazz = schema.getType(className);
      assertThat(clazz).isNotNull();
      result.close();

      result = db.command("sql", "create document type " + className + " if not exists");
      clazz = schema.getType(className);
      assertThat(clazz).isNotNull();
      result.close();
    });
  }
}
