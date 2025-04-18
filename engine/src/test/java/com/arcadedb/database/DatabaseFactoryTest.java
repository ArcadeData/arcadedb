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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.security.SecurityManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
class DatabaseFactoryTest extends TestHelper {

  @Test
  void invalidFactories() {
    try {
      new DatabaseFactory(null);
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    try {
      new DatabaseFactory("");
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }
  }

  @Test
  void testGetterSetter() {
    final DatabaseFactory f = new DatabaseFactory("test/");
    f.setAutoTransaction(true);
    assertThat(f.getContextConfiguration()).isNotNull();

    final SecurityManager security = new SecurityManager() {
      @Override
      public void updateSchema(final DatabaseInternal database) {
      }
    };
    f.setSecurity(security);
    assertThat(f.getSecurity()).isEqualTo(security);
  }

  @Test
  void testDatabaseRegistration() {
    final DatabaseFactory f = new DatabaseFactory("test");
    final Database db = f.create();

    DatabaseFactory.getActiveDatabaseInstances().contains(db);
    DatabaseFactory.removeActiveDatabaseInstance(db.getDatabasePath());
    assertThat(DatabaseFactory.getActiveDatabaseInstance(db.getDatabasePath())).isNull();

    db.drop();
    f.close();
  }

  @Test
  void testDatabaseRegistrationWithDifferentPathTypes() {
    final DatabaseFactory f = new DatabaseFactory("target/path/to/database");
    final Database db = f.exists() ? f.open() : f.create();

    assertThat(DatabaseFactory.getActiveDatabaseInstance("target/path/to/database")).isEqualTo(db);
    if (System.getProperty("os.name").toLowerCase().contains("windows"))
      assertThat(DatabaseFactory.getActiveDatabaseInstance("target\\path\\to\\database")).isEqualTo(db);
    assertThat(DatabaseFactory.getActiveDatabaseInstance("./target/path/../../target/path/to/database")).isEqualTo(db);

    db.drop();
    f.close();
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  void testDuplicatedDatabaseCreationWithDifferentPathTypes() {
    final DatabaseFactory f1 = new DatabaseFactory("path/to/database");
    final Database db = f1.create();

    final DatabaseFactory f2 = new DatabaseFactory(".\\path\\to\\database");
    assertThatExceptionOfType(DatabaseOperationException.class).isThrownBy(() -> f2.create());

    db.drop();
    f1.close();
    f2.close();
  }
}
