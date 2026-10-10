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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.ExternalValueRecord;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

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
class Issue9637ExternalBucketAclIT extends BaseGraphServerTest {
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
