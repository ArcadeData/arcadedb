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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12197: over gRPC a record set as a property value was stored as its toString() ("#1:0@Doc[name=target]") instead of
 * a link, and a property saved as null was missing from the record read back.
 */
class Issue12197GrpcRecordValueAndNullPropertyIT extends BaseGrpcClientServerTest {
  private static final String TYPE = "Doc12197";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void open() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE DOCUMENT TYPE `" + TYPE + "` IF NOT EXISTS");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aRecordSetAsAValueIsStoredAsALink() {
    final MutableDocument target = grpc.newDocument(TYPE);
    target.set("name", "target");
    target.save();
    final Document targetRead = grpc.lookupByRID(target.getIdentity(), true).asDocument();

    final MutableDocument doc = grpc.newDocument(TYPE);
    doc.set("owner", targetRead);
    doc.set("creator", target);
    doc.save();

    final Result row = grpc.query("sql",
        "SELECT owner.type() AS t, owner.name AS n, creator.type() AS ct, creator.name AS cn FROM " + doc.getIdentity()).next();
    assertThat(row.<String>getProperty("t")).isEqualTo("LINK");
    assertThat(row.<String>getProperty("n")).isEqualTo("target");
    assertThat(row.<String>getProperty("ct")).isEqualTo("LINK");
    assertThat(row.<String>getProperty("cn")).isEqualTo("target");
  }

  @Test
  void aPropertySavedAsNullIsReadBack() {
    final MutableDocument doc = grpc.newDocument(TYPE);
    doc.set("name", "x");
    doc.set("nothing", null);
    doc.save();

    final Document read = grpc.lookupByRID(doc.getIdentity(), true).asDocument();
    assertThat(read.has("nothing")).isTrue();
    assertThat(read.get("nothing")).isNull();
    assertThat(read.getPropertyNames()).contains("name", "nothing");
  }

  @Test
  void everyReadPathAnswersTheNullPropertyTheSame() {
    final MutableDocument doc = grpc.newDocument(TYPE);
    doc.set("name", "x");
    doc.set("nothing", null);
    doc.save();

    // the record stream shares the encoder of lookupByRID(), the query path has its own: all three must agree
    try (final ResultSet rs = grpc.queryStream("sql", "SELECT FROM " + doc.getIdentity())) {
      final Document streamed = rs.next().toElement();
      assertThat(streamed.has("nothing")).as("queryStream()").isTrue();
      assertThat(streamed.get("nothing")).isNull();
    }

    try (final ResultSet rs = grpc.query("sql", "SELECT FROM " + doc.getIdentity())) {
      final Document queried = rs.next().toElement();
      assertThat(queried.has("nothing")).as("query()").isTrue();
      assertThat(queried.get("nothing")).isNull();
    }
  }

  @Test
  void aRecordPassedAsAQueryParameterIsALink() {
    final MutableDocument target = grpc.newDocument(TYPE);
    target.set("name", "target");
    target.save();

    final MutableDocument doc = grpc.newDocument(TYPE);
    doc.set("owner", target.getIdentity());
    doc.save();

    // the parameters share the encoder of the properties: a record as a parameter is its link, alone or inside a list,
    // not its toString() (which would match no row)
    try (final ResultSet rs = grpc.query("sql", "SELECT FROM `" + TYPE + "` WHERE owner = :o", Map.of("o", target))) {
      assertThat(rs.hasNext()).as("a record as a parameter").isTrue();
      assertThat(rs.next().getIdentity().get()).isEqualTo(doc.getIdentity());
    }
    try (final ResultSet rs = grpc.query("sql", "SELECT FROM `" + TYPE + "` WHERE owner IN :os", Map.of("os", List.of(target)))) {
      assertThat(rs.hasNext()).as("a record inside a list parameter").isTrue();
      assertThat(rs.next().getIdentity().get()).isEqualTo(doc.getIdentity());
    }
  }
}
