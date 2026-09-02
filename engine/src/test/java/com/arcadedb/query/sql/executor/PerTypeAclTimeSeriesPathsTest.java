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
 * A TimeSeries type owns no record bucket, so the only thing that can gate it is the type-name ACL. Every path that
 * reaches its {@code TimeSeriesEngine} on behalf of a user - the plain scan, the {@code ts.timeBucket} push-down
 * aggregation, {@code INSERT}, the async sample append and the PromQL evaluator - has to apply that check, exactly
 * as {@code LocalBucket} applies the per-file one to a normal record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PerTypeAclTimeSeriesPathsTest {
  private static final String PATH             = "target/databases/PerTypeAclTimeSeriesPathsTest";
  private static final String RESTRICTED_TYPE  = "SecretMetrics";
  private static final String AUTHORIZED_TYPE  = "PublicMetrics";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();

    for (final String typeName : new String[] { RESTRICTED_TYPE, AUTHORIZED_TYPE }) {
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + typeName + " SET ts = 1000, value = 42.0");
        database.command("sql", "INSERT INTO " + typeName + " SET ts = 2000, value = 1337.0");
      });
    }
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    database.drop();
    factory.close();
  }

  @Test
  void aggregatePushDownDeniedOnRestrictedType() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final Throwable thrown = catchThrowable(() -> database.query("sql",
        "SELECT ts.timeBucket('1s', ts) AS b, avg(value) AS v FROM " + RESTRICTED_TYPE + " GROUP BY b").next());
    assertThat(thrown).as("the TimeSeries push-down aggregation must be gated like a plain scan of the same type")
        .isInstanceOf(SecurityException.class);

    final ResultSet allowed = database.query("sql",
        "SELECT ts.timeBucket('1s', ts) AS b, avg(value) AS v FROM " + AUTHORIZED_TYPE + " GROUP BY b");
    assertThat(allowed.hasNext()).as("the same aggregation on an authorized type must still work").isTrue();
  }

  @Test
  void insertDeniedOnRestrictedType() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.CREATE_RECORD);

    final Throwable thrown = catchThrowable(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO " + RESTRICTED_TYPE + " SET ts = 9999, value = -1.0")));
    assertThat(thrown).as("inserting a sample into a denied TimeSeries type must be rejected")
        .isInstanceOf(SecurityException.class);

    database.transaction(
        () -> database.command("sql", "INSERT INTO " + AUTHORIZED_TYPE + " SET ts = 9999, value = -1.0"));

    unbindUser();
    assertThat(database.countType(RESTRICTED_TYPE, false)).as("the rejected sample must not have been persisted")
        .isEqualTo(2);
    assertThat(database.countType(AUTHORIZED_TYPE, false)).as("the authorized insert must have been persisted")
        .isEqualTo(3);
  }

  @Test
  void asyncAppendSamplesDeniedOnRestrictedType() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.CREATE_RECORD);

    final Throwable thrown = catchThrowable(() -> database.async()
        .appendSamples(RESTRICTED_TYPE, new long[] { 9999L }, new Object[] { -1.0 }));
    assertThat(thrown).as("the async sample-append API must apply the same per-type check as INSERT")
        .isInstanceOf(SecurityException.class);

    database.async().appendSamples(AUTHORIZED_TYPE, new long[] { 9999L }, new Object[] { -1.0 });
    database.async().waitCompletion();

    unbindUser();
    assertThat(database.countType(RESTRICTED_TYPE, false)).as("the rejected sample must not have been persisted")
        .isEqualTo(2);
    // Without this the test would also pass if the authorized append had silently done nothing.
    assertThat(database.countType(AUTHORIZED_TYPE, false)).as("the authorized sample must have been persisted")
        .isEqualTo(3);
  }

  @Test
  void promQlEvaluationDeniedOnRestrictedType() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final Throwable thrown = catchThrowable(
        () -> database.query("sql", "SELECT promql('" + RESTRICTED_TYPE + "', 2000) AS r").next());
    assertThat(thrown).as("PromQL must not be a side door onto a denied TimeSeries type")
        .isInstanceOf(SecurityException.class);

    final Result allowed = database.query("sql", "SELECT promql('" + AUTHORIZED_TYPE + "', 2000) AS r").next();
    assertThat(allowed).as("PromQL on an authorized type must still work").isNotNull();
  }

  @Test
  void plainScanStillDeniedOnRestrictedType() {
    denyOn(RESTRICTED_TYPE, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final Throwable thrown = catchThrowable(() -> database.query("sql", "SELECT FROM " + RESTRICTED_TYPE).hasNext());
    assertThat(thrown).isInstanceOf(SecurityException.class);

    assertThat(database.query("sql", "SELECT FROM " + AUTHORIZED_TYPE).hasNext()).isTrue();
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
   * Grants everything except the listed {@code ACCESS} values on the listed type names, so a query that still
   * succeeds proves the check is scoped to the denied type and access rather than a blanket lockout.
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
