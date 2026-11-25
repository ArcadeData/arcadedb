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

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The per-type access of a TimeSeries type is registered under the canonical name of the type, and a type can also be
 * reached by one of its aliases. Every check made on behalf of a user has to be made on the name the type is registered
 * under, whichever of its names the caller used: counting, reading, writing and deleting samples through an alias must be
 * refused exactly as they are through the canonical name, and allowed exactly as they are when the type is granted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PerTypeAclTimeSeriesAliasTest {
  private static final String PATH             = "target/databases/PerTypeAclTimeSeriesAliasTest";
  private static final String RESTRICTED_TYPE  = "ReservedMetrics";
  private static final String RESTRICTED_ALIAS = "ReservedMetricsAlias";
  private static final String GRANTED_TYPE     = "OpenMetrics";
  private static final String GRANTED_ALIAS    = "OpenMetricsAlias";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();

    for (final String typeName : new String[] { RESTRICTED_TYPE, GRANTED_TYPE }) {
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + typeName + " SET ts = 1000, value = 42.0");
        database.command("sql", "INSERT INTO " + typeName + " SET ts = 2000, value = 1337.0");
      });
    }
    database.command("sql", "ALTER TYPE " + RESTRICTED_TYPE + " ALIASES " + RESTRICTED_ALIAS).close();
    database.command("sql", "ALTER TYPE " + GRANTED_TYPE + " ALIASES " + GRANTED_ALIAS).close();
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    database.drop();
    factory.close();
  }

  @Test
  void countThroughAliasRefusedLikeTheCanonicalName() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.READ_RECORD);

    assertThat(catchThrowable(() -> database.query("sql", "SELECT count(*) AS c FROM " + RESTRICTED_TYPE).next()))
        .as("counting a denied TimeSeries type by its name is refused").isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(() -> database.query("sql", "SELECT count(*) AS c FROM " + RESTRICTED_ALIAS).next()))
        .as("counting a denied TimeSeries type by its alias is refused the same way").isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(() -> database.countType(RESTRICTED_TYPE, false))).isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(() -> database.countType(RESTRICTED_ALIAS, false))).isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(() -> database.countType(RESTRICTED_ALIAS, true))).isInstanceOf(SecurityException.class);
  }

  @Test
  void countThroughAliasStillWorksOnAGrantedType() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.READ_RECORD);

    assertThat(database.query("sql", "SELECT count(*) AS c FROM " + GRANTED_ALIAS).next().<Long>getProperty("c"))
        .as("the alias of a granted type is counted").isEqualTo(2L);
    assertThat(database.query("sql", "SELECT count(*) AS c FROM " + GRANTED_TYPE).next().<Long>getProperty("c"))
        .isEqualTo(2L);
    assertThat(database.countType(GRANTED_ALIAS, false)).isEqualTo(2L);
  }

  @Test
  void countThroughAliasWithoutAnyUserStillWorks() {
    assertThat(database.countType(RESTRICTED_ALIAS, false)).isEqualTo(2L);
    assertThat(database.query("sql", "SELECT count(*) AS c FROM " + RESTRICTED_ALIAS).next().<Long>getProperty("c"))
        .isEqualTo(2L);
  }

  @Test
  void readThroughAliasRefusedLikeTheCanonicalName() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.READ_RECORD);

    assertThat(catchThrowable(() -> database.query("sql", "SELECT FROM " + RESTRICTED_ALIAS).hasNext()))
        .isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(() -> database.query("sql",
        "SELECT ts.timeBucket('1s', ts) AS b, avg(value) AS v FROM " + RESTRICTED_ALIAS + " GROUP BY b").next()))
        .isInstanceOf(SecurityException.class);
    assertThat(database.query("sql", "SELECT FROM " + GRANTED_ALIAS).hasNext()).isTrue();
  }

  @Test
  void writeThroughAliasRefusedLikeTheCanonicalName() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.CREATE_RECORD, SecurityDatabaseUser.ACCESS.DELETE_RECORD);

    assertThat(catchThrowable(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO " + RESTRICTED_ALIAS + " SET ts = 9999, value = -1.0"))))
        .isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(
        () -> database.command("sql", "DELETE FROM " + RESTRICTED_ALIAS + " WHERE ts < 1500").close()))
        .isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(() -> database.async()
        .appendSamples(RESTRICTED_ALIAS, new long[] { 9999L }, new Object[] { -1.0 })))
        .isInstanceOf(SecurityException.class);

    database.transaction(() -> database.command("sql", "INSERT INTO " + GRANTED_ALIAS + " SET ts = 9999, value = -1.0"));

    unbindUser();
    assertThat(database.countType(RESTRICTED_TYPE, false)).as("nothing was written to or removed from the denied type")
        .isEqualTo(2L);
    assertThat(database.countType(GRANTED_TYPE, false)).isEqualTo(3L);
  }

  private void denyOn(final String typeName, final SecurityDatabaseUser.ACCESS... accesses) {
    final Set<SecurityDatabaseUser.ACCESS> denied = EnumSet.noneOf(SecurityDatabaseUser.ACCESS.class);
    for (final SecurityDatabaseUser.ACCESS access : accesses)
      denied.add(access);

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(restrictedUser(Map.of(typeName, denied)));
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /**
   * Grants everything except the listed {@code ACCESS} values on the listed type names, and knows no other name than the
   * canonical ones, as the per-type map of a server user does: a name it has no entry for is allowed.
   */
  private SecurityDatabaseUser restrictedUser(final Map<String, Set<SecurityDatabaseUser.ACCESS>> deniedByType) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        final Set<ACCESS> denied = deniedByType.get(typeName);
        return denied == null || !denied.contains(access);
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
