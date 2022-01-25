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
package com.arcadedb.query.sql.functions.stat;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.function.misc.SQLFunctionUUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SQLFunctionUUIDTest {

  private SQLFunctionUUID uuid;

  @BeforeEach
  public void setup() {
    uuid = new SQLFunctionUUID();
  }

  @Test
  public void testEmpty() {
    Object result = uuid.getResult();
    assertNull(result);
  }

  @Test
  public void testResult() {
    String result = (String) uuid.execute(null, null, null, null, null);
    assertNotNull(result);
  }

  @Test
  public void testQuery() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionUUIDTest", (db) -> {
      ResultSet result = db.query("sql", "select uuid() as uuid");
      assertNotNull(result);
      assertNotNull(result.next().getProperty("uuid"));
    });
  }
}
