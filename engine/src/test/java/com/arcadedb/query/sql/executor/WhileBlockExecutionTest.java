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
public class WhileBlockExecutionTest extends TestHelper {
  public WhileBlockExecutionTest() {
    autoStartTx = true;
  }

  @Test
  public void testPlain() {

    String className = "testPlain";

    database.getSchema().createDocumentType(className);

    String script = "";
    script += "LET $i = 0;";
    script += "WHILE ($i < 3){\n";
    script += "  insert into " + className + " set value = $i;\n";
    script += "  LET $i = $i + 1;";
    script += "}";
    script += "SELECT FROM " + className + ";";

    ResultSet results = database.execute("sql", script);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      Result item = results.next();
      sum += (Integer) item.getProperty("value");
      tot++;
    }
    Assertions.assertEquals(3, tot);
    Assertions.assertEquals(3, sum);
    results.close();
  }

  @Test
  public void testReturn() {
    String className = "testReturn";

    database.getSchema().createDocumentType(className);

    String script = "";
    script += "LET $i = 0;";
    script += "WHILE ($i < 3){\n";
    script += "  insert into " + className + " set value = $i;\n";
    script += "  IF ($i = 1) {";
    script += "    RETURN;";
    script += "  }";
    script += "  LET $i = $i + 1;";
    script += "}";

    ResultSet results = database.execute("sql", script);
    results.close();
    results = database.query("sql", "SELECT FROM " + className);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      Result item = results.next();
      sum += (Integer) item.getProperty("value");
      tot++;
    }
    Assertions.assertEquals(2, tot);
    Assertions.assertEquals(1, sum);
    results.close();
  }
}
