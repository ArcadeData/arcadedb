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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5269.
 *
 * <p>{@code ServerSecurityDatabaseUser} builds a per-user file-access map when the security configuration is loaded.
 * A bucket/index file created at runtime (after that load) used to remain outside the map forever - until the next
 * type-drop or a server restart. Every access to such a file fell through the "not yet in security configuration,
 * allowing by default" path, which (a) logged at INFO on <b>every</b> access (thousands/sec under write load, rotating
 * the container logs within seconds), and (b) silently bypassed the per-type access rules configured for the group.</p>
 *
 * <p>The fix refreshes the security file-access map whenever the schema reaches a stable persisted state (i.e. after a
 * type/bucket/index is created), so the fallback path is no longer chronically hit. As a defense-in-depth measure the
 * fallback log line is now emitted at most once per fileId and demoted to FINE.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5269FileAccessRefreshIT {
  private static ArcadeDBServer SERVER;
  private static ServerSecurity SECURITY;
  private final static String   DATABASE_NAME = "Issue5269FileAccessRefreshIT";

  /**
   * Root cause: a bucket added to an existing type at runtime must be picked up by the already-built per-user security
   * map. Type creation was already covered (TypeBuilder calls updateSecurity), but adding a bucket (or an index) to an
   * existing type - the exact "new bucket/index file" the reporter observed - was not, so its fileId stayed outside the
   * map forever and every access fell through the allow-by-default path (log flood + per-type rule bypass).
   */
  @Test
  void runtimeCreatedBucketIsCoveredBySecurityMap() {
    SECURITY.createUser(new JSONObject().put("name", "reader").put("password", SECURITY.encodePassword("readerpwd"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "restrictedReader" }))));

    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);

      // 'SecretType' is denied read by the group (empty access list). Its first bucket is registered at type creation.
      database.getSchema().createDocumentType("SecretType");

      final ServerSecurityDatabaseUser dbu = SECURITY.getUser("reader").getDatabaseUser(database);
      final int firstBucketId = database.getSchema().getType("SecretType").getBuckets(false).getFirst().getFileId();
      assertThat(dbu.requestAccessOnFile(firstBucketId, SecurityDatabaseUser.ACCESS.READ_RECORD))
          .as("reader must be denied on the type's original bucket (mapped at type creation)").isFalse();

      // Add a NEW bucket to the existing type at runtime. This goes through saveConfiguration but NOT through the
      // TypeBuilder path that manually refreshed security, so before the fix the new bucket stayed outside the map.
      final Bucket extraBucket = database.getSchema().createBucket("SecretType_extra");
      database.getSchema().getType("SecretType").addBucket(extraBucket);
      final int extraBucketId = extraBucket.getFileId();

      assertThat(extraBucketId).isGreaterThan(firstBucketId);
      assertThat(dbu.requestAccessOnFile(extraBucketId, SecurityDatabaseUser.ACCESS.READ_RECORD))
          .as("reader must be denied on a bucket added to the type at runtime once the security map is refreshed")
          .isFalse();
    } finally {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);
      dropTypeIfExists(database, "SecretType");
      if (database.getSchema().existsBucket("SecretType_extra"))
        database.getSchema().dropBucket("SecretType_extra");
      SECURITY.dropUser("reader");
    }
  }

  /**
   * Defense in depth: even when the fallback path is reached (e.g. a file registered but not yet mapped within the same
   * open transaction), the informational line must be logged at most once per fileId, not per access.
   */
  @Test
  void fallbackAccessLineIsLoggedAtMostOncePerFile() {
    SECURITY.createUser(new JSONObject().put("name", "reader2").put("password", SECURITY.encodePassword("reader2pwd"))
        .put("databases", new JSONObject().put(DATABASE_NAME, new JSONArray(new String[] { "restrictedReader" }))));

    final AtomicInteger fallbackLines = new AtomicInteger();
    try {
      final DatabaseInternal database = SERVER.getDatabase(DATABASE_NAME);
      database.getSchema().createDocumentType("MappedType");

      final ServerSecurityDatabaseUser dbu = SECURITY.getUser("reader2").getDatabaseUser(database);

      LogManager.instance().setLogger(new FallbackLineCountingLogger(fallbackLines));

      // A fileId that is deliberately far beyond any registered file: it always takes the fallback branch.
      final int missingFileId = 1_000_000;
      boolean allowed = true;
      for (int i = 0; i < 500; i++)
        allowed &= dbu.requestAccessOnFile(missingFileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

      assertThat(allowed).as("a not-yet-registered file is allowed by default").isTrue();
      assertThat(fallbackLines.get()).as("the fallback line must be logged at most once per fileId, not per access")
          .isEqualTo(1);
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
      dropTypeIfExists(SERVER.getDatabase(DATABASE_NAME), "MappedType");
      SECURITY.dropUser("reader2");
    }
  }

  private static void dropTypeIfExists(final DatabaseInternal database, final String typeName) {
    if (database.getSchema().existsType(typeName))
      database.getSchema().dropType(typeName);
  }

  @BeforeAll
  static void beforeAll() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue("dD5ed08c");
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");

    SERVER = new ArcadeDBServer();
    SERVER.start();

    SECURITY = SERVER.getSecurity();
    // A group that grants read on any type by default, but explicitly denies read on 'SecretType' (empty access list).
    SECURITY.getDatabaseGroupsConfiguration(DATABASE_NAME).put("restrictedReader", new JSONObject().put("types",
        new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))
            .put("SecretType", new JSONObject().put("access", new JSONArray()))));
    SECURITY.saveGroups();

    SERVER.getOrCreateDatabase(DATABASE_NAME);
  }

  @AfterAll
  static void afterAll() {
    SERVER.stop();
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);

    FileUtils.deleteRecursively(new File("./target/config"));
    TestServerHelper.deleteDatabaseFolders(1);
    TestServerHelper.checkActiveDatabases();
    GlobalConfiguration.resetAll();
  }

  /**
   * Counts the "not yet in security configuration" fallback lines emitted by {@link ServerSecurityDatabaseUser}.
   */
  private static class FallbackLineCountingLogger implements Logger {
    private final AtomicInteger counter;

    private FallbackLineCountingLogger(final AtomicInteger counter) {
      this.counter = counter;
    }

    private void count(final Level level, final String message) {
      if (message != null && message.contains("not yet in security configuration"))
        counter.incrementAndGet();
    }

    @Override
    public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      count(iLevel, iMessage);
    }

    @Override
    public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException, final String context,
        final Object... args) {
      count(iLevel, iMessage);
    }

    @Override
    public void flush() {
    }
  }
}
