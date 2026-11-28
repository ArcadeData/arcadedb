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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A result set that stays open between two reads asks for the read grant of what it reads again when it takes the next batch of
 * rows, for the sources that answer from their own storage and not through the record loads of a bucket: the entries of an index
 * read as a target, and the samples of a time series type. A grant changed on the user in the meantime applies to the rest of
 * the result set, and not only to the statements that follow it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCursorGrantRecheckTest {
  private static final String PATH       = "target/databases/OpenCursorGrantRecheckTest";
  private static final String ENTRY_TYPE = "Entry";
  private static final String SERIES     = "Reading";
  // MORE THAN THE 100 ROWS A PULL OF A STEP ANSWERS
  private static final int    ROWS       = 500;

  private DatabaseFactory factory;
  private Database        database;
  private String          entryIndexName;
  private GrantedUser     user;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();

    database.command("sql", "CREATE DOCUMENT TYPE " + ENTRY_TYPE);
    database.command("sql", "CREATE PROPERTY " + ENTRY_TYPE + ".code STRING");
    database.command("sql", "CREATE INDEX ON " + ENTRY_TYPE + " (code) UNIQUE");
    entryIndexName = database.getSchema().getType(ENTRY_TYPE).getPolymorphicIndexByProperties("code").getName();
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.command("sql", "INSERT INTO " + ENTRY_TYPE + " SET code = 'c" + String.format("%04d", i) + "'");
    });

    database.command("sql", "CREATE TIMESERIES TYPE " + SERIES + " TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.command("sql", "INSERT INTO " + SERIES + " SET ts = " + (1000 + i) + ", value = " + i + ".0");
    });

    user = new GrantedUser();
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void indexEntriesStopAtTheNextBatchOnceTheGrantIsRemoved() {
    int delivered = 0;
    try (final ResultSet resultSet = database.query("sql", "SELECT key, rid FROM INDEX:`" + entryIndexName + "` WHERE key > ''")) {
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();
      ++delivered;

      user.revokeFile(database.getSchema().getType(ENTRY_TYPE).getBucketIds(false).getFirst());

      try {
        while (resultSet.hasNext()) {
          resultSet.next();
          ++delivered;
        }
        fail("the rest of the entries was served after the grant was removed");
      } catch (final SecurityException e) {
        assertThat(e.getMessage()).contains("not allowed");
      }
    }

    assertThat(delivered).isLessThan(ROWS);
  }

  @Test
  void indexEntriesAreServedInFullWhileTheGrantHolds() {
    int delivered = 0;
    try (final ResultSet resultSet = database.query("sql", "SELECT key, rid FROM INDEX:`" + entryIndexName + "` WHERE key > ''")) {
      while (resultSet.hasNext()) {
        resultSet.next();
        ++delivered;
      }
    }

    assertThat(delivered).isEqualTo(ROWS);
  }

  @Test
  void timeSeriesSamplesStopAtTheNextBatchOnceTheGrantIsRemoved() {
    int delivered = 0;
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM " + SERIES)) {
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();
      ++delivered;

      user.revokeType(SERIES);

      try {
        while (resultSet.hasNext()) {
          resultSet.next();
          ++delivered;
        }
        fail("the rest of the samples was served after the grant was removed");
      } catch (final SecurityException e) {
        assertThat(e.getMessage()).contains("not allowed");
      }
    }

    assertThat(delivered).isLessThan(ROWS);
  }

  @Test
  void timeSeriesSamplesAreServedInFullWhileTheGrantHolds() {
    int delivered = 0;
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM " + SERIES)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        ++delivered;
      }
    }

    assertThat(delivered).isEqualTo(ROWS);
  }

  /** A user whose grants are changed in place, as a refresh of the permissions of the server does on the cached user. */
  private static final class GrantedUser implements SecurityDatabaseUser {
    private final Set<Integer> deniedFiles = ConcurrentHashMap.newKeySet();
    private final Set<String>  deniedTypes = ConcurrentHashMap.newKeySet();

    void revokeFile(final int fileId) {
      deniedFiles.add(fileId);
    }

    void revokeType(final String typeName) {
      deniedTypes.add(typeName);
    }

    @Override
    public String getName() {
      return "reader";
    }

    @Override
    public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
      return true;
    }

    @Override
    public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
      return access != ACCESS.READ_RECORD || !deniedFiles.contains(fileId);
    }

    @Override
    public boolean requestAccessOnType(final String typeName, final ACCESS access) {
      return access != ACCESS.READ_RECORD || !deniedTypes.contains(typeName);
    }

    @Override
    public long getResultSetLimit() {
      return -1L;
    }

    @Override
    public long getReadTimeout() {
      return -1L;
    }
  }
}
