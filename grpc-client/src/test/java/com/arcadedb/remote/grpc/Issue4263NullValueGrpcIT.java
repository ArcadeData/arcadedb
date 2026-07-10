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
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4263: setting a property to {@code null} and saving it through the
 * gRPC {@link RemoteGrpcDatabase} client did not clear the value. The HTTP client worked fine.
 *
 * <p>Root cause: on the server, {@code convertWithSchemaType} converted a {@code null} incoming
 * value (carried over the wire as a {@code GrpcValue} with {@code KIND_NOT_SET}) into the literal
 * string {@code "null"} for STRING typed properties, because of the
 * {@code default -> String.valueOf(fromGrpcValue(v))} branch. The fix returns {@code null} for any
 * value that carries no kind, regardless of the schema type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue4263NullValueGrpcIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final int    HTTP_PORT   = 2480;
  private static final String VERTEX_TYPE = "SimpleVertexEx";
  private static final String EDGE_TYPE   = "SimpleVertexEx_ohmSVE";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, HTTP_PORT, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);

    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.svex IF NOT EXISTS STRING");
    grpc.command("sql", "CREATE EDGE TYPE `" + EDGE_TYPE + "` IF NOT EXISTS");
    grpc.command("sql", "DELETE FROM `" + VERTEX_TYPE + "`");
  }

  @AfterEach
  void closeClient() {
    if (grpc != null) {
      try {
        grpc.rollback();
      } catch (final Throwable ignore) {
        // ignore
      }
      grpc.close();
    }
    if (grpcServer != null)
      grpcServer.close();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void settingStringPropertyToNullClearsItOverGrpc() {
    // Mirrors the exact scenario reported in issue #4263, including the outgoing edge on svt1.
    grpc.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

    final MutableVertex svt1 = grpc.newVertex(VERTEX_TYPE);
    svt1.set("svex", "svt1");
    svt1.save();
    svt1.reload();

    final MutableVertex svt2 = grpc.newVertex(VERTEX_TYPE);
    svt2.set("svex", "svt2");
    svt2.save();
    svt1.newEdge(EDGE_TYPE, svt2);

    svt1.save();
    grpc.commit();

    assertThat(svt1.getString("svex")).isEqualTo("svt1");

    // Now clear the property.
    grpc.begin();
    svt1.set("svex", null);
    svt1.save();
    assertThat(svt1.getString("svex")).as("in-memory value before commit").isNull();
    grpc.commit();

    assertThat(svt1.getString("svex")).as("in-memory value after commit").isNull();

    // The bug: prior to the fix the server stored the literal string "null" for STRING properties,
    // so reload() (and a fresh query) would return "null" instead of a real null.
    svt1.reload();
    assertThat(svt1.getString("svex")).as("value after reload").isNull();
  }
}
