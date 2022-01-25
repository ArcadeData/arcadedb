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
public class ProfileStatementExecutionTest {
  @Test
  public void testProfile() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      db.getSchema().createDocumentType("testProfile");
      db.command("sql", "insert into testProfile set name ='foo'");
      db.command("sql", "insert into testProfile set name ='bar'");

      ResultSet result = db.query("sql", "PROFILE SELECT FROM testProfile WHERE name ='bar'");
      Assertions.assertTrue(result.getExecutionPlan().get().prettyPrint(0, 2).contains("μs"));

      result.close();
    });
  }
}
