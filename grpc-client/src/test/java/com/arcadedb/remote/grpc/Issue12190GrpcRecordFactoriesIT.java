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
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12190: the record factories of the Java remote client. {@link RemoteGrpcDatabase} inherits them from the HTTP client, so
 * it has to build an embedded document on the client, which its own {@code saveRecord()} then sends as an embedded value, and
 * refuse a document built on a vertex type and a vertex built on a document type, as the embedded API does.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class Issue12190GrpcRecordFactoriesIT extends BaseGrpcClientServerTest {

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    // HERE AND NOT IN A SEPARATE @AfterEach: THE ORDER OF TWO OF THEM IN ONE CLASS IS UNDEFINED, AND super.endTest() DROPS THE DATABASE
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE DOCUMENT TYPE Doc12190 IF NOT EXISTS");
    grpc.command("sql", "CREATE DOCUMENT TYPE Address12190 IF NOT EXISTS");
    grpc.command("sql", "CREATE VERTEX TYPE Person12190 IF NOT EXISTS");
    // declared embedded properties: the server types an embedded value after the schema
    grpc.command("sql", "CREATE PROPERTY Doc12190.address IF NOT EXISTS EMBEDDED OF Address12190");
    grpc.command("sql", "CREATE PROPERTY Address12190.previous IF NOT EXISTS EMBEDDED OF Address12190");
    grpc.command("sql", "CREATE PROPERTY Person12190.address IF NOT EXISTS EMBEDDED OF Address12190");
  }

  @Test
  void anEmbeddedDocumentIsSavedWithItsOwner() {
    final MutableDocument doc = grpc.newDocument("Doc12190");
    doc.set("name", "owner");
    final MutableEmbeddedDocument address = doc.newEmbeddedDocument("Address12190", "address");
    address.set("city", "Rome");
    address.newEmbeddedDocument("Address12190", "previous").set("city", "Milan");
    doc.save();

    try (final ResultSet rs = grpc.query("sql",
        "SELECT address.city AS city, address.previous.city AS previousCity FROM " + doc.getIdentity())) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("city")).isEqualTo("Rome");
      assertThat(row.<String>getProperty("previousCity")).isEqualTo("Milan");
    }

    // the server stored an embedded document of the declared type, not a plain map
    final EmbeddedDocument stored = getServerDatabase(0, getDatabaseName()).lookupByRID(doc.getIdentity(), true).asDocument()
        .getEmbedded("address");
    assertThat(stored.getTypeName()).isEqualTo("Address12190");
    assertThat(stored.getEmbedded("previous").getTypeName()).isEqualTo("Address12190");
  }

  @Test
  void anEmbeddedDocumentOfAVertexIsSavedWithItsOwner() {
    final MutableVertex vertex = grpc.newVertex("Person12190");
    vertex.newEmbeddedDocument("Address12190", "address").set("city", "Rome");
    vertex.save();

    try (final ResultSet rs = grpc.query("sql", "SELECT address.city AS city FROM " + vertex.getIdentity())) {
      assertThat(rs.next().<String>getProperty("city")).isEqualTo("Rome");
    }
    final EmbeddedDocument stored = getServerDatabase(0, getDatabaseName()).lookupByRID(vertex.getIdentity(), true).asDocument()
        .getEmbedded("address");
    assertThat(stored.getTypeName()).isEqualTo("Address12190");
  }

  @Test
  void theTypeKindIsChecked() {
    assertThatThrownBy(() -> grpc.newDocument("Person12190"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a document type");
    assertThatThrownBy(() -> grpc.newVertex("Doc12190"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a vertex type");
    assertThat(grpc.countType("Person12190", false)).isZero();
  }
}
