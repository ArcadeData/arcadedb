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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.DatabaseIsClosedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteDatabaseTest {

  private TestableRemoteDatabase database;

  /**
   * Testable subclass that overrides requestClusterConfiguration() to avoid HTTP calls.
   */
  static class TestableRemoteDatabase extends RemoteDatabase {
    TestableRemoteDatabase(final String server, final int port, final String databaseName,
        final String userName, final String userPassword) {
      super(server, port, databaseName, userName, userPassword, new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // No-op to avoid HTTP calls during tests
    }
  }

  @BeforeEach
  void setUp() {
    database = new TestableRemoteDatabase("localhost", 2480, "testdb", "root", "test");
  }

  @AfterEach
  void tearDown() {
    if (database.isOpen())
      database.close();
  }

  @Test
  void getName() {
    assertThat(database.getName()).isEqualTo("testdb");
  }

  @Test
  void getDatabasePath() {
    assertThat(database.getDatabasePath()).isEqualTo("http://localhost:2480/testdb");
  }

  @Test
  void toStringReturnsDatabaseName() {
    assertThat(database.toString()).isEqualTo("testdb");
  }

  @Test
  void isOpen() {
    assertThat(database.isOpen()).isTrue();
  }

  @Test
  void isOpenAfterClose() {
    database.close();
    assertThat(database.isOpen()).isFalse();
  }

  @Test
  void getSchema() {
    assertThat(database.getSchema()).isNotNull();
    assertThat(database.getSchema()).isInstanceOf(RemoteSchema.class);
  }

  @Test
  void getTransactionIsolationLevel() {
    assertThat(database.getTransactionIsolationLevel()).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
  }

  @Test
  void setTransactionIsolationLevel() {
    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(database.getTransactionIsolationLevel()).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
  }

  @Test
  void hashCodeIsConsistent() {
    final int hash1 = database.hashCode();
    final int hash2 = database.hashCode();
    assertThat(hash1).isEqualTo(hash2);
  }

  @Test
  void hashCodeUsesPath() {
    assertThat(database.hashCode()).isEqualTo(database.getDatabasePath().hashCode());
  }

  @Test
  void isTransactionActive() {
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void getNestedTransactions() {
    assertThat(database.getNestedTransactions()).isEqualTo(0);
  }

  @Test
  void checkDatabaseIsOpenWhenClosed() {
    database.close();
    assertThatThrownBy(() -> database.command("sql", "select 1"))
        .isInstanceOf(DatabaseIsClosedException.class);
  }

  // mapArgs tests

  @Test
  void mapArgsWithNull() {
    final Map<String, Object> result = database.mapArgs(null);
    assertThat(result).isNull();
  }

  @Test
  void mapArgsWithEmpty() {
    final Map<String, Object> result = database.mapArgs(new Object[]{});
    assertThat(result).isNull();
  }

  @Test
  void mapArgsWithSingleMap() {
    final Map<String, Object> params = new HashMap<>();
    params.put("name", "test");
    params.put("value", 42);

    final Map<String, Object> result = database.mapArgs(new Object[]{params});
    assertThat(result).isEqualTo(params);
  }

  @Test
  void mapArgsWithMultipleArgs() {
    final Map<String, Object> result = database.mapArgs(new Object[]{"a", "b", "c"});
    assertThat(result).hasSize(3);
    assertThat(result.get("0")).isEqualTo("a");
    assertThat(result.get("1")).isEqualTo("b");
    assertThat(result.get("2")).isEqualTo("c");
  }

  @Test
  void closeIsIdempotent() {
    database.close();
    database.close(); // Should not throw
    assertThat(database.isOpen()).isFalse();
  }

  @Test
  void acquireLock() {
    assertThat(database.acquireLock()).isNotNull();
    assertThat(database.acquireLock()).isInstanceOf(RemoteTransactionExplicitLock.class);
  }

  @Test
  void acquireLockReturnsSameInstance() {
    final var lock1 = database.acquireLock();
    final var lock2 = database.acquireLock();
    assertThat(lock1).isSameAs(lock2);
  }

  @Test
  void getSerializer() {
    assertThat(database.getSerializer()).isNotNull();
  }
}
