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
package com.arcadedb.server.security;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.ExternalValueRecord;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The read paths the engine opens on the user's behalf honor the user's grants: parallel scan producers, the
 * internal scan of a unidirectional edge lookup, and the paired bucket of external properties.
 */
class ReadPathAclIT {
  /**
   * Issue #8523: the producers of a parallel type scan read with the permissions of the user who ran the query, exactly
   * as the sequential scan does. A polymorphic scan is where it shows: the type in the FROM is checked when the query is
   * planned, the buckets of its subtypes when the scan opens them, which a parallel scan does on its producers. Pinned
   * for the plain scan, the filtered scan and the aggregation computed in the producers, against the same query run
   * sequentially.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ParallelScan extends BaseGraphServerTest {
    private static final String SCOPED_USER     = "scan-scoped-user";
    private static final String SCOPED_PWD      = "scanscopeduser1";
    private static final String RESTRICTED_TYPE = "SecretRows";
    private static final String AUTHORIZED_TYPE = "PublicRows";

    @Test
    void parallelScansActAsTheUserWhoRanTheQuery() throws Exception {
      testEachServer((serverIndex) -> {
        // AN AUTHORIZED PARENT WITH A DENIED SUBTYPE: THE PLANNER CHECKS THE PARENT, AND THE SUBTYPE'S BUCKETS ARE CHECKED
        // ONLY WHEN THE SCAN OPENS THEM - ON THE PRODUCERS WHEN IT RUNS IN PARALLEL. FOUR BUCKETS EACH: THE SCAN RUNS IN
        // PARALLEL WHATEVER THE PAGE-RANGE SETTINGS
        command(serverIndex, "CREATE DOCUMENT TYPE " + AUTHORIZED_TYPE + " BUCKETS 4");
        command(serverIndex, "CREATE DOCUMENT TYPE " + RESTRICTED_TYPE + " EXTENDS " + AUTHORIZED_TYPE + " BUCKETS 4");
        for (final String type : new String[] { AUTHORIZED_TYPE, RESTRICTED_TYPE })
          for (int x = 1; x <= 8; x++)
            command(serverIndex, "INSERT INTO " + type + " SET x = " + x);

        createScopedUser(serverIndex);
        try {
          final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());
          final ServerSecurityUser user = getServer(serverIndex).getSecurity().getUser(SCOPED_USER);
          for (final String template : QUERIES) {
            final String polymorphic = String.format(template, AUTHORIZED_TYPE);
            assertThat(explain(database, user, polymorphic)).as(polymorphic).contains("(parallel)");

            // OUTSIDE A TRANSACTION, AS THE SCOPED USER: THE PATH THAT RUNS ON THE PRODUCERS
            assertThatThrownBy(() -> DatabaseUserContext.runAs(database, user, () -> drain(database, polymorphic)))
                .as("'%s' reaches the denied subtype's buckets on the producers: it must be refused there too", polymorphic)
                .satisfies(e -> assertThat(rootCause(e)).isInstanceOf(SecurityException.class));

            // THE SAME QUERY RUN SEQUENTIALLY, INSIDE A TRANSACTION, IS REFUSED THE SAME WAY: THE PARALLEL SCAN MUST NOT
            // BE THE ONE PLACE WHERE THE SUBTYPE'S ROWS COME BACK
            assertThatThrownBy(() -> DatabaseUserContext.runAs(database, user, () -> {
              database.begin();
              try {
                return drain(database, polymorphic);
              } finally {
                database.rollback();
              }
            })).satisfies(e -> assertThat(rootCause(e)).isInstanceOf(SecurityException.class));
          }

          // POSITIVE CONTROL: THE SUBTYPE ALONE IS REFUSED BY THE PLANNER, THE PARENT'S OWN ROWS ARE READABLE AS root
          assertThat(drain(database, "SELECT FROM " + AUTHORIZED_TYPE)).isEqualTo(16);
        } finally {
          deleteUser(serverIndex);
        }
      });
    }

    private static final String[] QUERIES = { "SELECT FROM %s", "SELECT FROM %s WHERE x > 0", "SELECT sum(x) AS s FROM %s" };

    private static int drain(final DatabaseInternal database, final String query) {
      int rows = 0;
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
      }
      return rows;
    }

    private static String explain(final DatabaseInternal database, final ServerSecurityUser user, final String query) {
      return DatabaseUserContext.runAs(database, user, () -> {
        try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
          return rs.next().<String>getProperty("executionPlanAsString");
        }
      });
    }

    private static Throwable rootCause(Throwable e) {
      while (e.getCause() != null && e.getCause() != e)
        e = e.getCause();
      return e;
    }

    private void createScopedUser(final int serverIndex) throws Exception {
      final ServerSecurity security = getServer(serverIndex).getSecurity();
      security.getDatabaseGroupsConfiguration(getDatabaseName()).put("scanScoped",
          new JSONObject().put("access", new JSONArray())
              .put("types", new JSONObject()
                  .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
                  .put(RESTRICTED_TYPE, new JSONObject().put("access", new JSONArray()))));
      security.saveGroups();

      if (security.existsUser(SCOPED_USER))
        security.dropUser(SCOPED_USER);

      final JSONObject payload = new JSONObject().put("name", SCOPED_USER).put("password", SCOPED_PWD)
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("scanScoped")));
      final HttpURLConnection connection = open(serverIndex, "POST", "/api/v1/server/users", basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.getOutputStream().write(payload.toString().getBytes(StandardCharsets.UTF_8));
      connection.connect();
      try {
        assertThat(connection.getResponseCode()).isEqualTo(201);
      } finally {
        connection.disconnect();
      }
    }

    private void deleteUser(final int serverIndex) throws Exception {
      final HttpURLConnection connection = open(serverIndex, "DELETE",
          "/api/v1/server/users?name=" + URLEncoder.encode(SCOPED_USER, StandardCharsets.UTF_8), basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
      connection.connect();
      try {
        connection.getResponseCode();
      } finally {
        connection.disconnect();
      }
    }

    private HttpURLConnection open(final int serverIndex, final String method, final String path, final String auth) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:" + getServerHttpPort(serverIndex) + path).toURL()
          .openConnection();
      connection.setRequestMethod(method);
      connection.setRequestProperty("Authorization", auth);
      return connection;
    }

    private static String basicAuth(final String user, final String password) {
      return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Issue #8625: the scan that answers the incoming side of a unidirectional edge type is an internal read, so the
   * {@code resultSetLimit} of the user running the query must not cut it short - a partial scan would answer part of
   * the edges with no error, which is the failure the issue is about.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnidirectionalLookupResultSetLimit extends BaseGraphServerTest {
    private static final String GROUP   = "issue8625capped";
    private static final String USER    = "issue8625reader";
    private static final String PWD     = "issue8625pwd";
    private static final int    LIMIT   = 5;
    private static final int    SOURCES = 20;

    @Test
    void aCappedUserStillGetsEveryIncomingEdge() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.getSchema().createVertexType("Issue8625Q");
      database.getSchema().createVertexType("Issue8625T");
      database.getSchema().buildEdgeType().withName("Issue8625X").withBidirectional(false).create();
      database.transaction(() -> {
        final MutableVertex tag = database.newVertex("Issue8625T").set("name", "t").save();
        for (int i = 0; i < SOURCES; i++)
          database.newVertex("Issue8625Q").set("qid", i).save().newEdge("Issue8625X", tag);
      });

      final ServerSecurity security = getServer(0).getSecurity();
      security.getDatabaseGroupsConfiguration(getDatabaseName()).put(GROUP, new JSONObject()
          .put("resultSetLimit", LIMIT)
          .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
      security.saveGroups();
      if (security.existsUser(USER))
        security.dropUser(USER);
      security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PWD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(GROUP))));

      try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), USER, PWD)) {
        // Precondition: the cap bites on an ordinary scan
        long scanned = 0;
        try (final ResultSet rs = remote.query("sql", "SELECT FROM Issue8625Q")) {
          while (rs.hasNext()) {
            rs.next();
            ++scanned;
          }
        }
        assertThat(scanned).isEqualTo(LIMIT);

        // The source is left unlabelled, so the hop is walked from the bound target and answered by the scan
        try (final ResultSet rs = remote.query("opencypher",
            "MATCH (t:Issue8625T {name: 't'}) MATCH (t)<-[:Issue8625X]-(q) RETURN count(q) AS n")) {
          assertThat(((Number) rs.next().getProperty("n")).longValue())
              .as("the lookup must scan every edge, whatever the user's resultSetLimit").isEqualTo(SOURCES);
        }
      } finally {
        security.dropUser(USER);
        security.getDatabaseGroupsConfiguration(getDatabaseName()).remove(GROUP);
        security.saveGroups();
      }
    }
  }

  /**
   * Issue #9637: the per-file permission map a group's per-type grants are compiled into was keyed on the type's own buckets
   * only. The paired {@code <bucket>_ext} bucket, which holds the values of the type's EXTERNAL properties, was unlisted, so
   * every access on it fell through to default-allow: a user denied every access on the type could still read, update and
   * delete its large values by addressing the paired bucket directly.
   * <p>
   * The paired bucket now resolves to the type's grants, widened only by what an UPDATE of the owning record legitimately
   * does on it: an update-only role still creates the external record of a value that had none and deletes the one a value
   * set to null leaves behind.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExternalBucket extends BaseGraphServerTest {
    private static final String TYPE        = "Vault";
    private static final String DENIED_USER = "vault-denied";
    private static final String UPDATE_USER = "vault-updater";
    private static final String PASSWORD    = "vaultuser9637";
    private static final String BLOB        = "s".repeat(300);

    @Test
    void deniedUserCannotReachTheExternalValuesThroughThePairedBucket() {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
      final RID rid = createVaultWithOneRecord(database);
      final RID extRid = externalRids(database, rid).get("blob");
      final LocalBucket extBucket = database.getSchema().getEmbedded().getBucketById(extRid.getBucketId());

      createUser(DENIED_USER, "vaultDenied", new JSONArray());
      try {
        final ServerSecurityUser user = getServer(0).getSecurity().getUser(DENIED_USER);

        // POSITIVE CONTROL: THE PRIMARY RECORD IS REFUSED, SO THE GRANT IS IN EFFECT FOR THE TYPE
        assertRefused(database, user, () -> database.getSchema().getEmbedded().getBucketById(rid.getBucketId()).getRecord(rid));

        assertRefused(database, user, () -> extBucket.getRecord(extRid));
        assertRefused(database, user, () -> extBucket.iterator().hasNext());
        assertRefused(database, user, () -> {
          database.begin();
          try {
            extBucket.updateRecord(new ExternalValueRecord(database, extRid, new Binary(new byte[] { 1, 2, 3 })), true);
          } finally {
            database.rollback();
          }
          return null;
        });
        assertRefused(database, user, () -> {
          database.begin();
          try {
            extBucket.deleteRecord(extRid);
          } finally {
            database.rollback();
          }
          return null;
        });
      } finally {
        dropUser(DENIED_USER);
      }

      // NOTHING THE REFUSED CALLS TRIED STUCK
      assertThat(database.lookupByRID(rid, true).asDocument().getString("blob")).isEqualTo(BLOB);
    }

    @Test
    void updateOnlyRoleStillWritesEveryShapeOfExternalValue() {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
      final RID rid = createVaultWithOneRecord(database);

      createUser(UPDATE_USER, "vaultUpdater", new JSONArray().put("readRecord").put("updateRecord"));
      try {
        final ServerSecurityUser user = getServer(0).getSecurity().getUser(UPDATE_USER);

        // AN EXISTING EXTERNAL VALUE IS UPDATED IN PLACE, A NEW ONE IS CREATED IN THE PAIRED BUCKET
        DatabaseUserContext.runAs(database, user, () -> {
          database.transaction(() -> rid.asDocument(true).modify().set("blob", "u".repeat(300)).set("blob2", "n".repeat(300)).save());
          return null;
        });
        assertThat(externalRids(database, rid)).containsKeys("blob", "blob2");

        // A VALUE SET TO NULL HAS ITS EXTERNAL RECORD DELETED FROM THE PAIRED BUCKET
        DatabaseUserContext.runAs(database, user, () -> {
          database.transaction(() -> rid.asDocument(true).modify().set("blob2", null).save());
          return null;
        });
        assertThat(externalRids(database, rid)).containsOnlyKeys("blob");

        final Document reloaded = database.lookupByRID(rid, true).asDocument();
        assertThat(reloaded.getString("blob")).isEqualTo("u".repeat(300));
        assertThat(reloaded.getString("blob2")).isNull();

        // BUT IT IS NOT A READ GRANT IN DISGUISE NOR DOES IT LET THE ROLE DELETE THE OWNING RECORD
        assertRefused(database, user, () -> {
          database.transaction(() -> database.deleteRecord(rid.asDocument(true)));
          return null;
        });
      } finally {
        dropUser(UPDATE_USER);
      }
      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }

    /** The single-grant roles keep exactly the path to external values their grant names, now that the paired bucket is gated. */
    @Test
    void singleGrantRolesKeepTheirOwnPathToExternalValues() {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
      final RID rid = createVaultWithOneRecord(database);

      createUser("vault-reader", "vaultReader", new JSONArray().put("readRecord"));
      createUser("vault-creator", "vaultCreator", new JSONArray().put("createRecord"));
      createUser("vault-deleter", "vaultDeleter", new JSONArray().put("readRecord").put("deleteRecord"));
      try {
        final ServerSecurity security = getServer(0).getSecurity();

        // READ-ONLY: THE EXTERNAL VALUE IS MATERIALIZED THROUGH THE PAIRED BUCKET, BUT NOTHING CAN BE CHANGED
        final ServerSecurityUser reader = security.getUser("vault-reader");
        assertThat(DatabaseUserContext.runAs(database, reader, () -> database.lookupByRID(rid, true).asDocument().getString("blob")))
            .isEqualTo(BLOB);
        assertRefused(database, reader, () -> {
          database.transaction(() -> rid.asDocument(true).modify().set("blob", "r".repeat(300)).save());
          return null;
        });

        // CREATE-ONLY: A NEW RECORD'S EXTERNAL VALUE IS WRITTEN INTO THE PAIRED BUCKET
        final RID created = DatabaseUserContext.runAs(database, security.getUser("vault-creator"), () -> {
          final RID[] r = new RID[1];
          database.transaction(() -> r[0] = database.newDocument(TYPE).set("blob", "c".repeat(300)).save().getIdentity());
          return r[0];
        });
        assertThat(externalRids(database, created)).containsKey("blob");
        assertThat(database.lookupByRID(created, true).asDocument().getString("blob")).isEqualTo("c".repeat(300));

        // READ + DELETE: DELETING THE RECORD CASCADES TO ITS EXTERNAL VALUES
        final RID extRid = externalRids(database, rid).get("blob");
        final LocalBucket extBucket = database.getSchema().getEmbedded().getBucketById(extRid.getBucketId());
        DatabaseUserContext.runAs(database, security.getUser("vault-deleter"), () -> {
          database.transaction(() -> database.deleteRecord(rid.asDocument(true)));
          return null;
        });
        assertThat(database.existsRecord(rid)).isFalse();
        assertThat(extBucket.existsRecord(extRid)).isFalse();
      } finally {
        dropUser("vault-reader");
        dropUser("vault-creator");
        dropUser("vault-deleter");
      }
    }

    private RID createVaultWithOneRecord(final DatabaseInternal database) {
      final DocumentType type = database.getSchema().createDocumentType(TYPE, 1);
      type.createProperty("blob", Type.STRING).setExternal(true);
      type.createProperty("blob2", Type.STRING).setExternal(true);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument(TYPE).set("blob", BLOB).save().getIdentity());

      final LocalDocumentType localType = (LocalDocumentType) database.getSchema().getEmbedded().getType(TYPE);
      assertThat(localType.hasExternalBuckets()).isTrue();
      return rid[0];
    }

    private static Map<String, RID> externalRids(final DatabaseInternal database, final RID rid) {
      return database.getSerializer().findExistingExternalRids(database, database.lookupByRID(rid, true).asDocument());
    }

    private static void assertRefused(final DatabaseInternal database, final ServerSecurityUser user, final Supplier<?> action) {
      final Throwable refused = catchThrowable(() -> DatabaseUserContext.runAs(database, user, action));
      assertThat(rootCause(refused)).isInstanceOf(SecurityException.class);
    }

    private static Throwable rootCause(Throwable e) {
      while (e != null && e.getCause() != null && e.getCause() != e)
        e = e.getCause();
      return e;
    }

    private void createUser(final String name, final String group, final JSONArray vaultAccess) {
      final ServerSecurity security = getServer(0).getSecurity();
      ServerSecurityTestAccess.databaseGroups(security, getDatabaseName()).put(group,
          new JSONObject().put("access", new JSONArray())
              .put("types", new JSONObject()
                  .put("*", new JSONObject().put("access",
                      new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord")))
                  .put(TYPE, new JSONObject().put("access", vaultAccess))));
      security.saveGroups();

      if (security.existsUser(name))
        security.dropUser(name);

      security.createUser(new JSONObject()
          .put("name", name)
          .put("password", security.encodePassword(PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(group))));
    }

    private void dropUser(final String name) {
      final ServerSecurity security = getServer(0).getSecurity();
      if (security.existsUser(name))
        security.dropUser(name);
    }
  }
}
