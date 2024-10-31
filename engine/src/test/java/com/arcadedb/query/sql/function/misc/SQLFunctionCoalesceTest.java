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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionCoalesceTest extends TestHelper {

  @Test
  public void testBoolAnd_SingleNull() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "INSERT INTO doc (num) VALUES (1),(3),(5),(2),(4)");

      assertThat(database.countType("doc", true)).isEqualTo(5);

      ResultSet result = database.query("sql", "SELECT coalesce((SELECT num FROM doc)) as coal");
      assertThat(result.hasNext()).isTrue();

      List coal = result.next().getProperty("coal");
      assertThat(coal).hasSize(5);

      result = database.query("sql", "SELECT coalesce((SELECT num FROM doc ORDER BY num)) as coal");
      assertThat(result.hasNext()).isTrue();

      coal = result.next().getProperty("coal");
      assertThat(coal).hasSize(5);
    });
  }
}
