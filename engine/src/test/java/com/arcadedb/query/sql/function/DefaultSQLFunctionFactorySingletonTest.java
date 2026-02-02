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
package com.arcadedb.query.sql.function;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static com.arcadedb.TestHelper.checkActiveDatabases;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that DefaultSQLFunctionFactory and DefaultSQLMethodFactory are singletons,
 * ensuring functions are registered only once in FunctionRegistry.
 */
class DefaultSQLFunctionFactorySingletonTest {

  @BeforeEach
  void beforeEach() {
    checkActiveDatabases();
  }

  @AfterEach
  void afterEach() {
    checkActiveDatabases();
  }

  @Test
  void functionFactorySingleton() {
    final DefaultSQLFunctionFactory instance1 = DefaultSQLFunctionFactory.getInstance();
    final DefaultSQLFunctionFactory instance2 = DefaultSQLFunctionFactory.getInstance();
    assertThat(instance1).isSameAs(instance2);
  }

  @Test
  void methodFactorySingleton() {
    final DefaultSQLMethodFactory instance1 = DefaultSQLMethodFactory.getInstance();
    final DefaultSQLMethodFactory instance2 = DefaultSQLMethodFactory.getInstance();
    assertThat(instance1).isSameAs(instance2);
  }

  @Test
  void functionsRegisteredOnlyOnceWithMultipleDatabases() {
    // Record the current function count before creating databases
    final int initialCount = FunctionRegistry.size();

    // Create and close multiple databases
    for (int i = 0; i < 3; i++) {
      final String dbPath = "./target/databases/SingletonTest" + i;
      FileUtils.deleteRecursively(new File(dbPath));

      try (final Database db = new DatabaseFactory(dbPath).create()) {
        // Force query engine initialization by executing a query
        db.query("sql", "SELECT 1");
      }

      FileUtils.deleteRecursively(new File(dbPath));
    }

    // Verify function count hasn't changed (functions not re-registered)
    final int finalCount = FunctionRegistry.size();
    assertThat(finalCount).isEqualTo(initialCount);
  }

  @Test
  void sqlQueryEngineUsesSharedFactories() {
    final String dbPath = "./target/databases/SingletonSQLEngineTest";
    FileUtils.deleteRecursively(new File(dbPath));

    try (final Database db = new DatabaseFactory(dbPath).create()) {
      final SQLQueryEngine engine = (SQLQueryEngine) db.getQueryEngine("sql");

      // Verify the engine uses the singleton factory
      assertThat(engine.getFunctionFactory()).isSameAs(DefaultSQLFunctionFactory.getInstance());
      assertThat(engine.getMethodFactory()).isSameAs(DefaultSQLMethodFactory.getInstance());
    }

    FileUtils.deleteRecursively(new File(dbPath));
  }
}
