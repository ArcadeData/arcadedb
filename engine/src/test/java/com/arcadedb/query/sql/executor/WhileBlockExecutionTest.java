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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class WhileBlockExecutionTest extends TestHelper {
  public WhileBlockExecutionTest() {
    autoStartTx = true;
  }

  @Test
  public void testPlain() {

    final String className = "testPlain";

    database.getSchema().createDocumentType(className);

    String script = """
        LET $i = 0;
        WHILE ($i < 3){
          insert into %s set value = $i;
          LET $i = $i + 1;
        }
        SELECT FROM %s;
        """.formatted(className, className);
    final ResultSet results = database.command("sqlscript", script);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      final Result item = results.next();
      sum += item.<Integer>getProperty("value");
      tot++;
    }
    assertThat(tot).isEqualTo(3);
    assertThat(sum).isEqualTo(3);
    results.close();
  }

  @Test
  public void testReturn() {
    final String className = "testReturn";

    database.getSchema().createDocumentType(className);

    String script = """
        LET $i = 0;
        WHILE ($i < 3){
          insert into %s set value = $i;
          IF ($i = 1) {
            RETURN;
          }
          LET $i = $i + 1;
        }
        """.formatted(className);
    ResultSet results = database.command("sqlscript", script);
    results.close();
    results = database.query("sql", "SELECT FROM " + className);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      final Result item = results.next();
      sum += item.<Integer>getProperty("value");
      tot++;
    }
    assertThat(tot).isEqualTo(2);
    assertThat(sum).isEqualTo(1);
    results.close();
  }
}
