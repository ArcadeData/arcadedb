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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser.ACCESS;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * The per-type and per-bucket answers of a {@link ServerSecurityDatabaseUser} follow the schema as it is now: a type, a
 * bucket or an alias the permission maps were not built from is answered from the same group definitions the maps were
 * built from, and a denied or never configured principal keeps its fixed answer.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ServerSecurityDatabaseUserSchemaChangesTest extends TestHelper {
  private static final String   GROUP = "limited";
  private static final String[] ALL   = { "createRecord", "readRecord", "updateRecord", "deleteRecord" };

  @Test
  void deniedPrincipalIsRefusedOnEveryFileAndType() {
    final ServerSecurityDatabaseUser user = new ServerSecurityDatabaseUser(database.getName(), "denied", new String[] { GROUP }, true);

    assertThat(user.requestAccessOnFile(0, ACCESS.READ_RECORD)).isFalse();
    assertThat(user.requestAccessOnType("Granted", ACCESS.READ_RECORD)).isFalse();
  }

  @Test
  void principalWithoutPermissionMapsAllowsEveryFileAndType() {
    final ServerSecurityDatabaseUser user = newUser();

    assertThat(user.requestAccessOnFile(1000, ACCESS.CREATE_RECORD)).isTrue();
    assertThat(user.requestAccessOnType("Anything", ACCESS.DELETE_RECORD)).isTrue();

    // NO GROUP DEFINITIONS TO BUILD FROM: THE PRINCIPAL STAYS AS IT WAS
    user.updateFileAccess((DatabaseInternal) database, null);
    assertThat(user.requestAccessOnFile(1000, ACCESS.CREATE_RECORD)).isTrue();
    assertThat(user.requestAccessOnType("Anything", ACCESS.DELETE_RECORD)).isTrue();
  }

  @Test
  void typeAndBucketCreatedAfterTheMapsWereBuiltFollowTheGrants() {
    final DatabaseInternal databaseInternal = (DatabaseInternal) database;
    final ServerSecurityDatabaseUser user = newUser();
    user.updateFileAccess(databaseInternal, grantedOnly());

    database.getSchema().createDocumentType("Granted");
    database.getSchema().createDocumentType("Other");

    assertThat(user.requestAccessOnType("Granted", ACCESS.UPDATE_RECORD)).isTrue();
    assertThat(user.requestAccessOnType("Other", ACCESS.READ_RECORD)).isFalse();
    assertThat(user.requestAccessOnFile(firstBucketId("Granted"), ACCESS.CREATE_RECORD)).isTrue();
    assertThat(user.requestAccessOnFile(firstBucketId("Other"), ACCESS.READ_RECORD)).isFalse();
  }

  @Test
  void groupDefinitionsRefreshedAfterTheMapsWereBuiltAreTheOnesRebuiltFrom() {
    final DatabaseInternal databaseInternal = (DatabaseInternal) database;
    final ServerSecurityDatabaseUser user = newUser();
    user.updateFileAccess(databaseInternal, grantedOnly());

    database.getSchema().createDocumentType("Other");
    assertThat(user.requestAccessOnType("Other", ACCESS.READ_RECORD)).isFalse();

    // THE GRANTS CHANGE, THEN ANOTHER TYPE APPEARS: THE REBUILD USES THE NEWER DEFINITIONS
    user.updateFileAccess(databaseInternal, new JSONObject().put(GROUP,
        new JSONObject().put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray(ALL))))));
    database.getSchema().createDocumentType("Later");

    assertThat(user.requestAccessOnType("Later", ACCESS.READ_RECORD)).isTrue();
    assertThat(user.requestAccessOnFile(firstBucketId("Later"), ACCESS.READ_RECORD)).isTrue();
    assertThat(user.requestAccessOnType("Other", ACCESS.READ_RECORD)).isTrue();
  }

  @Test
  void fileMissingFromTheMapsIsAnsweredByTheTypeThatInvolvesIt() {
    database.getSchema().createDocumentType("Granted");
    database.getSchema().createDocumentType("Other");

    // THE MAPS ARE BUILT OVER A FILE MANAGER THAT LISTS NO FILE, SO NO FILE ID OF THE SCHEMA IS IN THEM
    final FileManager noFiles = mock(FileManager.class);
    final DatabaseInternal view = mock(DatabaseInternal.class);
    final Schema schema = database.getSchema();
    when(view.getSchema()).thenReturn(schema);
    when(view.getFileManager()).thenReturn(noFiles);

    final ServerSecurityDatabaseUser user = newUser();
    user.updateFileAccess(view, grantedOnly());
    assertThat(user.currentAccess().fileAccess()).isEmpty();

    assertThat(user.requestAccessOnFile(firstBucketId("Granted"), ACCESS.DELETE_RECORD)).isTrue();
    assertThat(user.requestAccessOnFile(firstBucketId("Other"), ACCESS.READ_RECORD)).isFalse();

    // A FILE NO TYPE INVOLVES IS RESTRICTED BY NO TYPE, ASKED TWICE FOR THE SAME FILE
    assertThat(user.requestAccessOnFile(100_000, ACCESS.READ_RECORD)).isTrue();
    assertThat(user.requestAccessOnFile(100_000, ACCESS.READ_RECORD)).isTrue();
  }

  @Test
  void aliasAndNameOfNoTypeAreAnsweredLikeTheTypeTheyWillBe() {
    database.getSchema().createDocumentType("Granted").setAliases(Set.of("GrantedAlias"));
    database.getSchema().createDocumentType("Other").setAliases(Set.of("OtherAlias"));

    final ServerSecurityDatabaseUser user = newUser();
    user.updateFileAccess((DatabaseInternal) database, grantedOnly());

    assertThat(user.requestAccessOnType("GrantedAlias", ACCESS.CREATE_RECORD)).isTrue();
    assertThat(user.requestAccessOnType("OtherAlias", ACCESS.READ_RECORD)).isFalse();

    // A NAME NO TYPE HAS YET IS ANSWERED FROM THE DEFINITIONS, AS THE TYPE WILL BE ONCE IT IS CREATED
    assertThat(user.requestAccessOnType("NotCreatedYet", ACCESS.READ_RECORD)).isFalse();
    assertThat(user.requestAccessOnType("Granted", ACCESS.READ_RECORD)).isTrue();
  }

  private ServerSecurityDatabaseUser newUser() {
    return new ServerSecurityDatabaseUser(database.getName(), "limitedUser", new String[] { GROUP });
  }

  private int firstBucketId(final String typeName) {
    return database.getSchema().getType(typeName).getBuckets(false).getFirst().getFileId();
  }

  /** Every access to the type {@code Granted}, nothing on any other type. */
  private static JSONObject grantedOnly() {
    return new JSONObject().put(GROUP, new JSONObject().put("types", new JSONObject()
        .put("Granted", new JSONObject().put("access", new JSONArray(ALL)))
        .put("*", new JSONObject().put("access", new JSONArray()))));
  }
}
