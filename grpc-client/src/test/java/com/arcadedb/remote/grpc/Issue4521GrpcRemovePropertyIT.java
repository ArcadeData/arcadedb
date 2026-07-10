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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #4521: removing a property from a record and saving it works over HTTP but the property
 * is left behind when the same operation is performed over gRPC. The gRPC client used to send the update as a
 * partial (merge) payload, which cannot express a dropped property, and the server merged it instead of
 * replacing the record content.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4521GrpcRemovePropertyIT extends BaseGraphServerTest {

  private static final String VERTEX_TYPE = "SimpleVertexEx";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.s IF NOT EXISTS STRING");
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      if (grpc != null) {
        try {
          grpc.command("sql", "DELETE FROM `" + VERTEX_TYPE + "`");
        } catch (final Throwable ignore) {
        }
        grpc.close();
      }
      if (grpcServer != null)
        grpcServer.close();
    } finally {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      super.endTest();
    }
  }

  @Test
  void removePropertyOverGrpcClearsIt() {
    // Create the vertex with property "s"
    grpc.begin();
    final MutableVertex svt1 = grpc.newVertex(VERTEX_TYPE);
    svt1.set("s", "svt1");
    svt1.save();
    grpc.commit();

    final RID rid = svt1.getIdentity();
    assertThat(rid).isNotNull();
    assertThat(svt1.getString("s")).isEqualTo("svt1");

    // Remove property "s" and save
    grpc.begin();
    svt1.remove("s");
    svt1.save();
    grpc.commit();

    // After reload the property must be gone (this was the bug: it stayed "svt1" over gRPC)
    svt1.reload();
    assertThat(svt1.getString("s")).isNull();
  }
}
