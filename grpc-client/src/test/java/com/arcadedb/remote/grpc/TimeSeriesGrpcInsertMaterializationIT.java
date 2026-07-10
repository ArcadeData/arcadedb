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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression: a single-row SQL {@code INSERT} into a TIME SERIES type, executed through the gRPC
 * client, must materialise its response without throwing. Time-series points are non-addressable
 * (no @rid), so the server returns the inserted row with an empty proto rid field. The client copied
 * that empty string into the metadata map, and {@code RemoteImmutableDocument} then called
 * {@code newRID("")}, throwing "The RID '' is not valid" on every TS insert even though the point was
 * already committed. Proto3 scalars cannot be null, so the gRPC client must treat an empty proto
 * rid/type as absent (matching the HTTP/JSON contract where the metadata key is simply missing).
 */
class TimeSeriesGrpcInsertMaterializationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT = 50051;
  private static final String TYPE_NAME = "sensor";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase db;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (db != null) {
      db.close();
      db = null;
    }
    if (grpcServer != null) {
      grpcServer.close();
      grpcServer = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void tsInsertOverGrpcClientMaterialisesWithoutThrowing() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    db = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    db.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (sensor_id STRING, region STRING) FIELDS (temperature DOUBLE, humidity DOUBLE)");

    // Before the fix this threw "The RID '' is not valid" while materialising the inserted row.
    assertThatCode(() -> {
      try (final ResultSet rs = db.command("sql",
          "INSERT INTO " + TYPE_NAME + " SET ts = ?, sensor_id = ?, region = ?, temperature = ?, humidity = ?",
          1_000L, "sensor-0", "region-0", 15.0, 40.0)) {
        // Materialise the row: this is where the empty @rid was rejected.
        while (rs.hasNext())
          rs.next();
      }
    }).doesNotThrowAnyException();

    final ResultSet count = db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME);
    assertThat(((Number) count.next().getProperty("cnt")).longValue()).isEqualTo(1L);
  }
}
