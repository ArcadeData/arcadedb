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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A user denied {@code readRecord}/{@code deleteRecord} on a type must not be able to reach that type's data (or
 * its TimeSeries samples, or its index entries) through a query plan that never loads the record through its
 * bucket - {@code INDEX:} targets, the {@code MAX}/{@code MIN}/{@code count(*)} index shortcuts, and the TimeSeries
 * read/count paths all have to apply the same per-type check {@code LocalBucket} applies to a normal record load.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PerTypeAclIndexAndTimeSeriesTest {
  private static final String PATH        = "target/databases/PerTypeAclIndexAndTimeSeriesTest";
  private static final String SECRET_TYPE = "Secret";
  private static final String PUBLIC_TYPE = "Public";
  private static final String TS_TYPE     = "SecretMetrics";

  private DatabaseFactory factory;
  private Database        database;
  private String          secretIndexName;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();

    database.command("sql", "CREATE DOCUMENT TYPE " + SECRET_TYPE);
    database.command("sql", "CREATE PROPERTY " + SECRET_TYPE + ".ssn STRING");
    database.command("sql", "CREATE INDEX ON " + SECRET_TYPE + " (ssn) UNIQUE");
    secretIndexName = database.getSchema().getType(SECRET_TYPE).getPolymorphicIndexByProperties("ssn").getName();
    database.transaction(() -> database.command("sql", "INSERT INTO " + SECRET_TYPE + " SET ssn = '123-45-6789'"));

    database.command("sql", "CREATE DOCUMENT TYPE " + PUBLIC_TYPE);
    database.command("sql", "CREATE PROPERTY " + PUBLIC_TYPE + ".code STRING");
    database.command("sql", "CREATE INDEX ON " + PUBLIC_TYPE + " (code) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO " + PUBLIC_TYPE + " SET code = 'ok'"));

    database.command("sql", "CREATE TIMESERIES TYPE " + TS_TYPE + " TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO " + TS_TYPE + " SET ts = 1000, value = 42.0"));
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void indexTargetSelectDeniedOnRestrictedTypeGrantedOnPublicType() {
    bindUser(Set.of(SECRET_TYPE), Set.of());

    final Throwable thrown = catchThrowable(
        () -> database.query("sql", "SELECT key, rid FROM INDEX:`" + secretIndexName + "` WHERE key > ''").hasNext());
    assertThat(thrown).as("reading a denied type's indexed values directly must be gated like a normal record read")
        .isInstanceOf(SecurityException.class);

    final ResultSet publicRs = database.query("sql", "SELECT key FROM INDEX:`" + publicIndexName() + "` WHERE key > ''");
    assertThat(publicRs.hasNext()).as("the same query on an authorized type must still work").isTrue();
  }

  @Test
  void indexTargetDeleteDeniedOnRestrictedType() {
    bindUser(Set.of(SECRET_TYPE), Set.of());

    final Throwable thrown = catchThrowable(() -> database.transaction(
        () -> database.command("sql", "DELETE FROM INDEX:`" + secretIndexName + "` WHERE key = '123-45-6789'")));
    assertThat(thrown).as("deleting a denied type's index entry must be gated, not silently desync the index")
        .isInstanceOf(SecurityException.class);

    unbindUser();
    final long entries = database.getSchema().getIndexByName(secretIndexName).countEntries();
    assertThat(entries).as("no index entry may have been removed by the rejected delete").isEqualTo(1);
  }

  @Test
  void maxMinIndexShortcutDeniedOnRestrictedType() {
    bindUser(Set.of(SECRET_TYPE), Set.of());

    // MaxMinFromIndexStep (like CountFromIndexStep and the TS count below) computes its single row lazily on the
    // first next() - hasNext() only reports "not executed yet" - so the scan has to be driven past hasNext() for
    // the check inside next() to run.
    final Throwable thrown = catchThrowable(() -> database.query("sql", "SELECT max(ssn) FROM " + SECRET_TYPE).next());
    assertThat(thrown).as("the MAX/MIN index shortcut must be gated exactly like a full record scan would be")
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void countIndexShortcutDeniedOnRestrictedType() {
    bindUser(Set.of(SECRET_TYPE), Set.of());

    final Throwable thrown = catchThrowable(
        () -> database.query("sql", "SELECT count(*) FROM INDEX:`" + secretIndexName + "`").next());
    assertThat(thrown).as("counting a denied type's index entries must be gated like counting its records")
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void timeSeriesReadAndCountDeniedOnRestrictedType() {
    bindUser(Set.of(), Set.of(TS_TYPE));

    final Throwable readThrown = catchThrowable(() -> database.query("sql", "SELECT FROM " + TS_TYPE).hasNext());
    assertThat(readThrown).as("reading a denied TimeSeries type's samples must be gated like a normal record read")
        .isInstanceOf(SecurityException.class);

    final Throwable countThrown = catchThrowable(() -> database.query("sql", "SELECT count(*) FROM " + TS_TYPE).next());
    assertThat(countThrown).as("counting a denied TimeSeries type's samples must be gated too")
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void schemaIndexesListingHidesTheDeniedTypeAndKeepsThePublicOne() {
    bindUser(Set.of(SECRET_TYPE), Set.of());

    final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes");
    final List<String> visibleIndexNames = new ArrayList<>();
    while (rs.hasNext())
      visibleIndexNames.add(rs.next().<String>getProperty("name"));

    assertThat(visibleIndexNames).as("a denied type's index must not even be listed")
        .doesNotContain(secretIndexName);
    assertThat(visibleIndexNames).as("an authorized type's index must still be listed")
        .contains(publicIndexName());
  }

  private String publicIndexName() {
    return database.getSchema().getType(PUBLIC_TYPE).getPolymorphicIndexByProperties("code").getName();
  }

  private void bindUser(final Set<String> deniedTypesByBucket, final Set<String> deniedTypesByName) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(restrictedUser(deniedTypesByBucket, deniedTypesByName));
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /**
   * Denies every {@code ACCESS} on the buckets of {@code deniedTypesByBucket} and, independently, on the type names
   * in {@code deniedTypesByName} (the only check a bucket-less TimeSeries type can be gated by) - everything else is
   * granted, so a query succeeding proves the fix is scoped to the denied type and not a blanket lockout.
   */
  private SecurityDatabaseUser restrictedUser(final Set<String> deniedTypesByBucket, final Set<String> deniedTypesByName) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypesByBucket)
      for (final int bucketId : database.getSchema().getType(typeName).getBucketIds(false))
        deniedBucketIds.add(bucketId);

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
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return !deniedTypesByName.contains(typeName);
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
