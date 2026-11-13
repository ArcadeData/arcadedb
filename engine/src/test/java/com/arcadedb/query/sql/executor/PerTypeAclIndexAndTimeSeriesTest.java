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
import com.arcadedb.database.RID;
import com.arcadedb.security.SecurityDatabaseUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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

  /**
   * The Cypher schema procedures answer from the schema rather than from records, so they have to hide a type the
   * user cannot read exactly as {@code schema:types} does, instead of naming it and its property keys.
   */
  @Test
  void cypherSchemaProceduresHideRestrictedType() {
    bindUser(Set.of(SECRET_TYPE), Set.of());

    final List<String> propertyKeys = new ArrayList<>();
    database.query("opencypher", "CALL db.propertyKeys()").forEachRemaining(r -> propertyKeys.add(r.getProperty("propertyKey")));
    assertThat(propertyKeys).contains("code").doesNotContain("ssn");

    final List<String> typeNames = new ArrayList<>();
    database.query("opencypher", "CALL db.schema.visualization()").forEachRemaining(r -> typeNames.add(r.getProperty("name")));
    assertThat(typeNames).contains(PUBLIC_TYPE).doesNotContain(SECRET_TYPE);

    unbindUser();
    final List<String> allKeys = new ArrayList<>();
    database.query("opencypher", "CALL db.propertyKeys()").forEachRemaining(r -> allKeys.add(r.getProperty("propertyKey")));
    assertThat(allKeys).as("with no user bound every type stays visible").contains("code", "ssn");
  }

  /**
   * A type the user can read may extend one it cannot: {@code schema:types} lists the child but must not name the
   * hidden parent in its {@code parentTypes}.
   */
  @Test
  void schemaTypesHidesUnreadableParentType() {
    database.command("sql", "CREATE DOCUMENT TYPE SecretChild EXTENDS " + SECRET_TYPE);
    bindUser(Set.of(SECRET_TYPE), Set.of());

    final List<Result> rows = new ArrayList<>();
    database.query("sql", "SELECT FROM schema:types WHERE name = 'SecretChild'").forEachRemaining(rows::add);
    assertThat(rows).hasSize(1);
    final List<String> parents = rows.getFirst().getProperty("parentTypes");
    assertThat(parents).doesNotContain(SECRET_TYPE);

    unbindUser();
    final List<String> allParents = database.query("sql", "SELECT FROM schema:types WHERE name = 'SecretChild'").next()
        .getProperty("parentTypes");
    assertThat(allParents).contains(SECRET_TYPE);
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

  // ---- #7477: scanning a LIGHTWEIGHT edge type applies the per-type ACL the bucket scan used to apply ----

  /**
   * Issue #7477, reported as discussion #7473: scanning a LIGHTWEIGHT edge type now reads the vertices instead of
   * the edge type's (empty) bucket, so the per-type check that the bucket scan performed for free has to be performed
   * by the walk - on the edge type the caller named, and on every vertex type the walk opens.
   * <p>
   * A {@code @Nested} class with its own database and lifecycle: its fixture needs a VERTEX type named {@code Secret},
   * while the enclosing class's setup creates a DOCUMENT type with that name.
   */
  @Nested
  class LightweightEdgeScan {
    private static final String PATH = "target/databases/PerTypeAclIndexAndTimeSeriesTest_LightweightEdgeScan";

    private DatabaseFactory factory;
    private Database        database;
    private RID             workSource;
    private RID             secretSource;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();

      database = factory.create();

      // Spread over four buckets on purpose: denying one of them is an ACL shape a per-type check cannot express.
      database.command("sql", "CREATE VERTEX TYPE Work BUCKETS 4");
      database.command("sql", "CREATE VERTEX TYPE Secret");
      database.command("sql", "CREATE EDGE TYPE Cite LIGHTWEIGHT");

      database.transaction(() -> {
        workSource = database.newVertex("Work").set("id", 0).save().getIdentity();
        final RID target = database.newVertex("Work").set("id", 1).save().getIdentity();
        secretSource = database.newVertex("Secret").set("id", 2).save().getIdentity();

        database.lookupByRID(workSource, true).asVertex().modify().newEdge("Cite", target);
        database.lookupByRID(secretSource, true).asVertex().modify().newEdge("Cite", target);
      });
    }

    @AfterEach
    void tearDown() {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
      database.drop();
      factory.close();
    }

    /** Regression test for issue #7477. */
    @Test
    void aUserDeniedTheEdgeTypeCannotReachItsEdgesThroughTheVertices() {
      bindUser(Set.of("Cite"));

      final Throwable thrown = catchThrowable(() -> database.query("sql", "SELECT FROM Cite").hasNext());
      assertThat(thrown).as("the walk replaces a bucket scan that was gated: the gate has to move with it")
          .isInstanceOf(SecurityException.class);
    }

    /**
     * Regression test for issue #7477.
     * <p>
     * A lightweight edge is only visible through the vertices that hold it, so the ones behind a denied vertex type
     * are not the caller's to see. Leaving that type out of the walk - rather than failing the whole statement over an
     * ACL on a type the query never named - keeps the edge type queryable for what the caller may read.
     */
    @Test
    void aDeniedVertexTypeIsLeftOutOfTheWalkRatherThanFailingIt() {
      bindUser(Set.of("Secret"));

      final List<String> pairs = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM Cite")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          pairs.add(r.getEdge().get().getOut() + "->" + r.getEdge().get().getIn());
        }
      }

      assertThat(pairs).hasSize(1);
      assertThat(pairs.getFirst()).startsWith(workSource + "->");
    }

    /**
     * Regression test for issue #7477.
     * <p>
     * A type-level check answers yes as soon as ONE bucket of the type is readable, so a vertex type with several
     * buckets and a per-bucket ACL would pass it and then fail inside {@code BucketIterator} on the first denied
     * bucket - failing the whole statement over a type it never named, which is exactly what leaving a denied type
     * out of the walk is meant to avoid. The walk therefore checks one bucket at a time (issue #7477).
     */
    @Test
    void oneDeniedBucketOfAReadableVertexTypeIsSkipped() {
      assertThat(database.getSchema().getType("Work").getBucketIds(false).size())
          .as("precondition: Work must have more than one bucket, or a per-type check would already refuse it")
          .isGreaterThan(1);

      // Exactly the bucket that holds the source of the Work->Work edge, so the type-level check still answers yes.
      bindUserOnBuckets(Set.of(workSource.getBucketId()));

      final List<String> pairs = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM Cite")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          pairs.add(r.getEdge().get().getOut() + "->" + r.getEdge().get().getIn());
        }
      }

      // The denied bucket's edge is dropped, the Secret vertex's is still reached, and nothing threw.
      assertThat(pairs).hasSize(1);
      assertThat(pairs.getFirst()).startsWith(secretSource + "->");
    }

    /**
     * Regression test for issue #7477.
     * <p>
     * The walk emits a lightweight subtype's edges out of a scan of its supertype, so the subtype has to be gated
     * too. It is today, and it would still be by accident even without the explicit check - the record scan opens a
     * BucketIterator per bucket of the hierarchy, and each of those checks. This pins the guarantee to the walk so
     * that skipping the record scan on a purely lightweight type could not silently take the ACL with it
     * (issue #7477).
     */
    @Test
    void aDeniedLightweightSubtypeIsRefusedThroughItsSupertype() {
      database.command("sql", "CREATE EDGE TYPE Mentions");
      database.command("sql", "CREATE EDGE TYPE Quotes EXTENDS Mentions LIGHTWEIGHT");
      database.transaction(() -> database.lookupByRID(secretSource, true).asVertex().modify()
          .newEdge("Quotes", workSource));

      bindUser(Set.of("Quotes"));

      final Throwable thrown = catchThrowable(() -> database.query("sql", "SELECT FROM Mentions").hasNext());
      assertThat(thrown).as("a subtype the caller cannot read must not be reachable through its supertype's scan")
          .isInstanceOf(SecurityException.class);
    }

    /** Regression test for issue #7477. */
    @Test
    void anUnrestrictedUserSeesEveryEdge() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM Cite")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
      }
    }

    private void bindUser(final Set<String> deniedTypes) {
      final Set<Integer> deniedBucketIds = new HashSet<>();
      for (final String typeName : deniedTypes)
        deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

      bindUser(deniedTypes, deniedBucketIds);
    }

    /** Denies the given buckets and no type by name: the per-bucket ACL shape a type-level check cannot express. */
    private void bindUserOnBuckets(final Set<Integer> deniedBucketIds) {
      bindUser(Set.of(), deniedBucketIds);
    }

    private void bindUser(final Set<String> deniedTypes, final Set<Integer> deniedBucketIds) {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
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
          return !deniedTypes.contains(typeName);
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }
  }
}
