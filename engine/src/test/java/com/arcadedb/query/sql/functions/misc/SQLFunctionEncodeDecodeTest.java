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
package com.arcadedb.query.sql.functions.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.function.misc.SQLFunctionDecode;
import com.arcadedb.query.sql.function.misc.SQLFunctionEncode;
import com.arcadedb.query.sql.function.misc.SQLFunctionUUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SQLFunctionEncodeDecodeTest {

  private SQLFunctionEncode encode;
  private SQLFunctionDecode decode;

  @BeforeEach
  public void setup() {
    encode = new SQLFunctionEncode();
    decode = new SQLFunctionDecode();
  }

  @Test
  public void testEmpty() {
    final Object result = encode.getResult();
    assertNull(result);
  }

  @Test
  public void testResult() {
    final String result = (String) encode.execute(null, null, null, new Object[] { "abc123", "base64" }, null);
    assertNotNull(result);
  }

  @Test
  public void testQuery() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionEncodeTest", (db) -> {
      final ResultSet result = db.query("sql", "select decode( encode('abc123', 'base64'), 'base64' ).asString() as encode");
      assertNotNull(result);
      final Object prop = result.next().getProperty("encode");
      assertEquals("abc123", prop);
    });
  }
}
