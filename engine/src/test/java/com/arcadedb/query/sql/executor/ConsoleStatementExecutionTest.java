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
import com.arcadedb.exception.CommandExecutionException;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 */
public class ConsoleStatementExecutionTest extends TestHelper {

  @Test
  public void testError() {
    ResultSet result = database.command("sqlscript", "console.`error` 'foo bar'");
    assertThat(Optional.ofNullable(result)).isNotNull();
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("level")).isEqualTo("error");
    assertThat(item.<String>getProperty("message")).isEqualTo("foo bar");
  }

  @Test
  public void testLog() {
    ResultSet result = database.command("sqlscript", "console.log 'foo bar'");
    assertThat(Optional.ofNullable(result)).isNotNull();
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("level")).isEqualTo("log");
    assertThat(item.<String>getProperty("message")).isEqualTo("foo bar");
  }

  @Test
  public void testInvalidLevel() {
    try {
      database.command("sqlscript", "console.bla 'foo bar'");
      fail("");
    } catch (CommandExecutionException x) {
      // EXPECTED
    } catch (Exception x2) {
      fail("");
    }
  }
}
