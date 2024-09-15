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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class DropTypeStatementExecutionTest extends TestHelper {
  @Test
  public void testPlain() {
    final String className = "testPlain";
    final Schema schema = database.getSchema();
    schema.createDocumentType(className);

    assertThat(schema.getType(className)).isNotNull();

    final ResultSet result = database.command("sql", "drop type " + className);
    assertThat(result.hasNext()).isTrue();
    final Result next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("drop type");
    assertThat(result.hasNext()).isFalse();
    result.close();

    assertThat(schema.existsType(className)).isFalse();
  }

  @Test
  public void testIfExists() {
    final String className = "testIfExists";
    final Schema schema = database.getSchema();
    schema.createDocumentType(className);

    assertThat(schema.getType(className)).isNotNull();

    ResultSet result = database.command("sql", "drop type " + className + " if exists");
    assertThat(result.hasNext()).isTrue();
    final Result next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("drop type");
    assertThat(result.hasNext()).isFalse();
    result.close();

    assertThat(schema.existsType(className)).isFalse();

    result = database.command("sql", "drop type " + className + " if exists");
    result.close();

    assertThat(schema.existsType(className)).isFalse();
  }

  @Test
  public void testParam() {
    final String className = "testParam";
    final Schema schema = database.getSchema();
    schema.createDocumentType(className);

    assertThat(schema.getType(className)).isNotNull();

    final ResultSet result = database.command("sql", "drop type ?", className);
    assertThat(result.hasNext()).isTrue();
    final Result next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("drop type");
    assertThat(result.hasNext()).isFalse();
    result.close();

    assertThat(schema.existsType(className)).isFalse();
  }
}
