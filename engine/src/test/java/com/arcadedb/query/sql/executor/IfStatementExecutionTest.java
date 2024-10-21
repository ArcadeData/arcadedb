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
public class IfStatementExecutionTest extends TestHelper {

  @Test
  public void testPositive() {
    final ResultSet results = database.command("sql", "if(1=1){ select 1 as a; }");
    assertThat(results.hasNext()).isTrue();
    final Result result = results.next();
    assertThat((Integer) result.getProperty("a")).isEqualTo(1);
    assertThat(results.hasNext()).isFalse();
    results.close();
  }

  @Test
  public void testNegative() {
    final ResultSet results = database.command("sql", "if(1=2){ select 1 as a; }");
    assertThat(results.hasNext()).isFalse();
    results.close();
  }

  @Test
  public void testIfReturn() {
    final ResultSet results = database.command("sql", "if(1=1){ return 'yes'; }");
    assertThat(results.hasNext()).isTrue();
    assertThat(results.next().<String>getProperty("value")).isEqualTo("yes");
    assertThat(results.hasNext()).isFalse();
    results.close();
  }
}
