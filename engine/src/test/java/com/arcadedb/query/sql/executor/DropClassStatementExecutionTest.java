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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class DropClassStatementExecutionTest extends TestHelper {
  @Test
  public void testPlain() {
    String className = "testPlain";
    Schema schema = database.getSchema();
    schema.createDocumentType(className);

    Assertions.assertNotNull(schema.getType(className));

    ResultSet result = database.command("sql", "drop type " + className);
    Assertions.assertTrue(result.hasNext());
    Result next = result.next();
    Assertions.assertEquals("drop type", next.getProperty("operation"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    Assertions.assertFalse(schema.existsType(className));
  }

  @Test
  public void testIfExists() {
    String className = "testIfExists";
    Schema schema = database.getSchema();
    schema.createDocumentType(className);

    Assertions.assertNotNull(schema.getType(className));

    ResultSet result = database.command("sql", "drop type " + className + " if exists");
    Assertions.assertTrue(result.hasNext());
    Result next = result.next();
    Assertions.assertEquals("drop type", next.getProperty("operation"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    Assertions.assertFalse(schema.existsType(className));

    result = database.command("sql", "drop type " + className + " if exists");
    result.close();

    Assertions.assertFalse(schema.existsType(className));
  }

  @Test
  public void testParam() {
    String className = "testParam";
    Schema schema = database.getSchema();
    schema.createDocumentType(className);

    Assertions.assertNotNull(schema.getType(className));

    ResultSet result = database.command("sql", "drop type ?", className);
    Assertions.assertTrue(result.hasNext());
    Result next = result.next();
    Assertions.assertEquals("drop type", next.getProperty("operation"));
    Assertions.assertFalse(result.hasNext());
    result.close();

    Assertions.assertFalse(schema.existsType(className));
  }
}
