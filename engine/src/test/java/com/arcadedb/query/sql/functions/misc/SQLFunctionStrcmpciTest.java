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
import com.arcadedb.query.sql.function.misc.SQLFunctionStrcmpci;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionStrcmpciTest {

  private SQLFunctionStrcmpci function;

  @BeforeEach
  public void setup() {
    function = new SQLFunctionStrcmpci();
  }

  @Test
  public void testEmpty() {
    Object result = function.getResult();
    Assertions.assertNull(result);
  }

  @Test
  public void testResult() {
    Assertions.assertEquals(0, function.execute(null, null, null, new String[] { "ThisIsATest", "THISISATEST" }, null));
  }

  @Test
  public void testQuery() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionStrcmpci", (db) -> {
      ResultSet result = db.query("sql", "select strcmpci('ThisIsATest', 'THISISATEST') as strcmpci");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(0, (Integer) result.next().getProperty("strcmpci"));

      result = db.query("sql", "select strcmpci(null, null) as strcmpci");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(0, (Integer) result.next().getProperty("strcmpci"));

      result = db.query("sql", "select strcmpci('ThisIsATest', null) as strcmpci");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(1, (Integer) result.next().getProperty("strcmpci"));

      result = db.query("sql", "select strcmpci(null, 'ThisIsATest') as strcmpci");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(-1, (Integer) result.next().getProperty("strcmpci"));

      result = db.query("sql", "select strcmpci('ThisIsATest', 'THISISATESTO') as strcmpci");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(-1, (Integer) result.next().getProperty("strcmpci"));

      result = db.query("sql", "select strcmpci('ThisIsATestO', 'THISISATEST') as strcmpci");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(+1, (Integer) result.next().getProperty("strcmpci"));

      result = db.query("sql", "select strcmpci('ThisIsATestO', 'THISISATESTE') as strcmpci");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(+1, (Integer) result.next().getProperty("strcmpci"));
    });
  }
}
