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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class BeginStatementExecutionTest {
  @Test
  public void testBegin() throws Exception {
    TestHelper.executeInNewDatabase("OCommitStatementExecutionTest", (db) -> {
      Assertions.assertTrue(db.getTransaction() == null || !db.getTransaction().isActive());
      ResultSet result = db.command("sql", "begin");
      //printExecutionPlan(null, result);
      Assertions.assertNotNull(result);
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertEquals("begin", item.getProperty("operation"));
      Assertions.assertFalse(result.hasNext());
      Assertions.assertFalse(db.getTransaction() == null || !db.getTransaction().isActive());
      db.commit();
    });
  }
}
