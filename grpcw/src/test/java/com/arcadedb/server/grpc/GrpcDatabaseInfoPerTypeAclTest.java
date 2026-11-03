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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import com.arcadedb.utility.FileUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code getDatabaseInfo} read the schema and counted the records of every type without binding the caller, so the engine's
 * per-type gates were no-ops for it: a user granted the database but denied some of its types by per-type permissions was
 * handed the census of those types too, and, once bound, a refused count would have named the type among the uncounted ones.
 * The census is now read with the caller bound and covers only the types it may read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrpcDatabaseInfoPerTypeAclTest extends BaseGraphServerTest {
  private static final String NAME         = "GrpcInfoPerTypeAcl";
  private static final String SCOPED_USER  = "grpcinfoscoped";
  private static final String SCOPED_PWD   = "grpcinfoscoped-pwd";

  private ArcadeDbGrpcAdminService service;

  @BeforeEach
  void createDatabase() {
    service = new ArcadeDbGrpcAdminService(getServer(0), new DefaultCredentialsValidator());
    final ServerDatabase database = getServer(0).createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    database.getSchema().createDocumentType("Doc");
    database.getSchema().createDocumentType("Secret").createProperty("code", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Secret", "code");
    database.command("sql", "CREATE TIMESERIES TYPE SecretMetric TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.command("sql", "INSERT INTO Doc SET k = ?", i);
      database.command("sql", "INSERT INTO Secret SET code = 'a'");
      database.command("sql", "INSERT INTO Secret SET code = 'b'");
      database.command("sql", "INSERT INTO SecretMetric SET ts = 1000, host = 'h', v = 1");
    });

    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(NAME, "perTypeScoped",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
                .put("Secret", new JSONObject().put("access", new JSONArray()))
                .put("SecretMetric", new JSONObject().put("access", new JSONArray()))));
    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);
    security.createUser(new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", security.encodePassword(SCOPED_PWD))
        .put("databases", new JSONObject().put(NAME, new JSONArray().put("perTypeScoped"))));
  }

  @AfterEach
  void dropDatabase() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);
    security.deleteGroup(NAME, "perTypeScoped");
    if (getServer(0).existsDatabase(NAME))
      getServer(0).dropLocalDatabase(getServer(0).getDatabaseForOperator(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(
        new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  @Test
  void theCensusCoversOnlyTheTypesTheCallerMayRead() {
    final GetDatabaseInfoResponse all = databaseInfo("root", DEFAULT_PASSWORD_FOR_TESTS);
    assertThat(all.getClasses()).isEqualTo(3);
    assertThat(all.getRecords()).isEqualTo(6L);
    assertThat(all.getIndexes()).isGreaterThan(0);

    final GetDatabaseInfoResponse scoped = databaseInfo(SCOPED_USER, SCOPED_PWD);
    assertThat(scoped.getClasses()).as("the types the user may read").isEqualTo(1);
    assertThat(scoped.getRecords()).as("the records of the types the user may read").isEqualTo(3L);
    assertThat(scoped.getIndexes()).as("no index of a type the user may read").isZero();
    assertThat(scoped.getUncountedTypesList()).as("a denied type is not named").isEmpty();
  }

  private GetDatabaseInfoResponse databaseInfo(final String user, final String password) {
    final CollectingObserver<GetDatabaseInfoResponse> observer = new CollectingObserver<>();
    service.getDatabaseInfo(GetDatabaseInfoRequest.newBuilder().setCredentials(
        DatabaseCredentials.newBuilder().setUsername(user).setPassword(password).build()).setName(NAME).build(), observer);
    assertThat(observer.error).as("the call failed").isNull();
    assertThat(observer.completed).isTrue();
    assertThat(observer.values).hasSize(1);
    return observer.values.getFirst();
  }

  private static final class CollectingObserver<T> implements StreamObserver<T> {
    private final List<T> values = new ArrayList<>();
    private Throwable     error;
    private boolean       completed;

    @Override
    public void onNext(final T value) {
      values.add(value);
    }

    @Override
    public void onError(final Throwable t) {
      error = t;
    }

    @Override
    public void onCompleted() {
      completed = true;
    }
  }
}
