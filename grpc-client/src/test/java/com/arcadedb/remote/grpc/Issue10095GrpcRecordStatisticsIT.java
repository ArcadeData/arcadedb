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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10095, the remote half over gRPC: {@code RemoteGrpcDatabase.saveRecord} incremented {@code createRecord} before
 * branching into its UPDATE arm, so every update was counted as a create and {@code updateRecord} stayed at zero for the
 * whole session. Each arm now counts its own statistic, once the server confirmed the write.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10095GrpcRecordStatisticsIT extends BaseGrpcClientServerTest {

  private static final String DOC_TYPE = "Doc10095";

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
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE DOCUMENT TYPE `" + DOC_TYPE + "` IF NOT EXISTS");
  }

  @AfterEach
  void closeClient() {
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
  }

  @Test
  void anUpdateIsCountedAsAnUpdateAndNotAsACreate() {
    final long creates = stat("createRecord");
    final long updates = stat("updateRecord");

    final MutableDocument doc = grpc.newDocument(DOC_TYPE).set("v", 0);
    final RID rid = doc.save().getIdentity();
    assertThat(rid).isNotNull();
    assertThat(stat("createRecord")).as("the first save creates").isEqualTo(creates + 1);
    assertThat(stat("updateRecord")).isEqualTo(updates);

    doc.set("v", 1).save();
    assertThat(stat("createRecord")).as("saving an existing record is not a create").isEqualTo(creates + 1);
    assertThat(stat("updateRecord")).as("it is an update").isEqualTo(updates + 1);

    grpc.newDocument(DOC_TYPE).set("v", 2);
    assertThat(stat("createRecord")).as("an instance that was not saved is not counted").isEqualTo(creates + 1);
  }

  private long stat(final String name) {
    return ((Number) grpc.getStats().get(name)).longValue();
  }
}
