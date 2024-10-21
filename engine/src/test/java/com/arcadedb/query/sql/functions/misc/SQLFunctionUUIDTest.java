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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.function.misc.SQLFunctionUUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SQLFunctionUUIDTest {

  private SQLFunctionUUID uuid;

  @BeforeEach
  public void setup() {
    uuid = new SQLFunctionUUID();
  }

  @Test
  public void testEmpty() {
    final Object result = uuid.getResult();
    assertThat(result).isNull();
  }

  @Test
  public void testResult() {
    final String result = (String) uuid.execute(null, null, null, null, null);
    assertThat(result).isNotNull();
  }

  @Test
  public void testQuery() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionUUIDTest", (db) -> {
      final ResultSet result = db.query("sql", "select uuid() as uuid");
      assertThat((Iterator<? extends Result>) result).isNotNull();
      assertThat(result.next().<String>getProperty("uuid")).isNotNull();
    });
  }
}
