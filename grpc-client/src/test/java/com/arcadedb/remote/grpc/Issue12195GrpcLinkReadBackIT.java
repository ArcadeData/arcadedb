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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12195: the gRPC client decoded every link as its rid text, so a LINK property read back as a String and
 * {@code (Identifiable) doc.get(name)} failed.
 */
class Issue12195GrpcLinkReadBackIT extends BaseGrpcClientServerTest {
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
    grpc.command("sql", "CREATE VERTEX TYPE P12195 IF NOT EXISTS");
    grpc.command("sql", "CREATE EDGE TYPE K12195 IF NOT EXISTS");
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
  void linksAreReadBackAsRids() {
    final RID other = grpc.command("sql", "INSERT INTO P12195 SET name = 'other'").next().getIdentity().get();
    final RID me = grpc.command("sql", "INSERT INTO P12195 SET name = 'me', friend = " + other + ", friends = [" + other + "]").next()
        .getIdentity().get();

    final Document doc = grpc.lookupByRID(me, true).asDocument();
    assertThat(doc.get("friend")).isEqualTo(other);
    assertThat(((Identifiable) doc.get("friend")).getIdentity()).isEqualTo(other);
    assertThat(doc.<Object>getList("friends")).containsExactly(other);

    final Result row = grpc.query("sql", "SELECT friend, friends FROM " + me).next();
    assertThat(row.<Object>getProperty("friend")).isEqualTo(other);
    assertThat(row.<List<Object>>getProperty("friends")).containsExactly(other);
  }

  @Test
  void edgesAndRidProjectionsStillWork() {
    final RID a = grpc.command("sql", "INSERT INTO P12195 SET name = 'a'").next().getIdentity().get();
    final RID b = grpc.command("sql", "INSERT INTO P12195 SET name = 'b'").next().getIdentity().get();
    grpc.command("sql", "CREATE EDGE K12195 FROM " + a + " TO " + b);

    final Edge edge = grpc.query("sql", "SELECT FROM K12195").next().getEdge().get();
    assertThat(edge.getOut()).isEqualTo(a);
    assertThat(edge.getIn()).isEqualTo(b);

    final Vertex vertex = grpc.lookupByRID(a, true).asVertex();
    assertThat(vertex.countEdges(Vertex.DIRECTION.OUT, "K12195")).isEqualTo(1);

    final Result row = grpc.query("sql", "SELECT @rid, @type, name FROM P12195 WHERE name = 'a'").next();
    assertThat(row.getIdentity()).hasValue(a);
    assertThat(row.<String>getProperty("name")).isEqualTo("a");
  }
}
