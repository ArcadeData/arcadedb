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
import com.arcadedb.security.SecurityManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
class DatabaseFactoryTest extends TestHelper {

  @Test
  void invalidFactories() {
    try {
      new DatabaseFactory(null);
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }

    try {
      new DatabaseFactory("");
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }
  }

  @Test
  void testGetterSetter() {
    final DatabaseFactory f = new DatabaseFactory("test/");
    f.setAutoTransaction(true);
    Assertions.assertNotNull(f.getContextConfiguration());

    SecurityManager security = new SecurityManager() {
      @Override
      public void updateSchema(DatabaseInternal database) {
      }
    };
    f.setSecurity(security);
    Assertions.assertEquals(security, f.getSecurity());
  }

  @Test
  void testDatabaseRegistration() {
    final DatabaseFactory f = new DatabaseFactory("test");
    Database db = f.create();

    DatabaseFactory.getActiveDatabaseInstances().contains(db);
    DatabaseFactory.removeActiveDatabaseInstance(db.getDatabasePath());
    Assertions.assertNull(DatabaseFactory.getActiveDatabaseInstance(db.getDatabasePath()));

    db.drop();
    f.close();
  }
}
