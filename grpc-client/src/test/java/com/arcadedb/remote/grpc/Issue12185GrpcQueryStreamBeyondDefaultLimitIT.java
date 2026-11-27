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
package com.arcadedb.remote.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.grpc.GrpcServerPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12185 on a gRPC connection: {@link RemoteGrpcDatabase} does not override {@code queryStream(String, String, Map)}
 * and {@code commandStream(String, String, Map)}, so those two stream over HTTP through the inherited
 * {@code RemoteDatabase} code, which stopped at the server's default row limit while the gRPC overloads, and
 * {@code countType()}, answered every row. The server default is lowered to 50 so 120 records cross it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12185GrpcQueryStreamBeyondDefaultLimitIT extends BaseGrpcClientServerTest {
  private static final int    CAP       = 50;
  private static final int    RECORDS   = 120;
  private static final String TYPE_NAME = "Item12185";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:" + GrpcServerPlugin.class.getName());
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT, CAP);
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    final Database embedded = getServerDatabase(0, getDatabaseName());
    embedded.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    embedded.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        embedded.newDocument(TYPE_NAME).set("i", i).save();
    });

    // Per test, after the server started: the gRPC port is assigned by the operating system on every start (#8209)
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null)
      database.close();
    if (grpcServer != null)
      grpcServer.close();
    super.endTest();
  }

  @Test
  void theInheritedHttpQueryStreamOverloadYieldsEveryRow() {
    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(RECORDS);
    try (final ResultSet rs = database.queryStream("sql", "SELECT i FROM " + TYPE_NAME, Map.of())) {
      assertThat(rs.stream().count()).isEqualTo(RECORDS);
    }
  }

  @Test
  void theInheritedHttpCommandStreamOverloadYieldsEveryRow() {
    try (final ResultSet rs = database.commandStream("sql", "SELECT i FROM " + TYPE_NAME, Map.of())) {
      assertThat(rs.stream().count()).isEqualTo(RECORDS);
    }
  }

  @Test
  void theGrpcQueryStreamAndIterateTypeAgreeWithTheHttpOverload() {
    try (final ResultSet rs = database.queryStream("sql", "SELECT i FROM " + TYPE_NAME)) {
      assertThat(rs.stream().count()).isEqualTo(RECORDS);
    }

    final Iterator<Record> iterator = database.iterateType(TYPE_NAME, true);
    long n = 0;
    while (iterator.hasNext()) {
      iterator.next();
      n++;
    }
    assertThat(n).isEqualTo(RECORDS);
  }
}
