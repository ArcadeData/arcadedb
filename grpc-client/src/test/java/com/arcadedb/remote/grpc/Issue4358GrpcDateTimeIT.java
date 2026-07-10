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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4358: setting a LocalDateTime or LocalDate property on a vertex via
 * RemoteGrpcDatabase.newVertex().set(...) threw NumberFormatException because
 * ProtoUtils.toGrpcValue(LocalDateTime) had no explicit branch and fell through to
 * setStringValue(value.toString()), producing an ISO 8601 string that the server
 * then tried to parse as a Long epoch millisecond value.
 */
public class Issue4358GrpcDateTimeIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final int    HTTP_PORT   = 2480;
  private static final String VERTEX_TYPE = "SimpleVertex4358";

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
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, HTTP_PORT, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.fecha IF NOT EXISTS DATETIME");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.startDate IF NOT EXISTS DATE");
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
  void localDateTimePropertyRoundTripsViaGrpc() {
    // Truncate to millis: DATETIME stores millisecond precision by default
    final LocalDateTime target = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);

    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("fecha", target);
    v.save();
    grpc.commit();

    try (final ResultSet rs = grpc.query("sql", "SELECT fecha FROM `" + VERTEX_TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      final Object fechaRaw = r.getProperty("fecha");
      assertThat(fechaRaw).as("'fecha' property must be present").isNotNull();

      // The gRPC wire layer returns TIMESTAMP_VALUE as epoch milliseconds (Long).
      // Convert back to LocalDateTime at UTC for comparison.
      final LocalDateTime fechaBack;
      switch (fechaRaw) {
        case LocalDateTime ldt -> fechaBack = ldt;
        case Date d -> fechaBack = d.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
        case Long epochMs -> fechaBack = LocalDateTime.ofEpochSecond(epochMs / 1000, (int) ((epochMs % 1000) * 1_000_000), ZoneOffset.UTC);
        case null, default -> throw new AssertionError("Unexpected type for 'fecha': " + fechaRaw.getClass());
      }

      assertThat(fechaBack).isEqualTo(target);
    }
  }

  @Test
  void localDatePropertyRoundTripsViaGrpc() {
    final LocalDate targetDate = LocalDate.now();

    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("startDate", targetDate);
    v.save();
    grpc.commit();

    try (final ResultSet rs = grpc.query("sql", "SELECT startDate FROM `" + VERTEX_TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      final Object dateRaw = r.getProperty("startDate");
      assertThat(dateRaw).as("'startDate' property must be present").isNotNull();

      // The gRPC wire layer returns TIMESTAMP_VALUE as epoch milliseconds (Long).
      // Convert back to LocalDate at UTC for comparison.
      final LocalDate dateBack;
      switch (dateRaw) {
        case LocalDate ld -> dateBack = ld;
        case Date d -> dateBack = d.toInstant().atOffset(ZoneOffset.UTC).toLocalDate();
        case Long epochMs -> dateBack = LocalDate.ofEpochDay(epochMs / 86_400_000L);
        case null, default -> throw new AssertionError("Unexpected type for 'startDate': " + dateRaw.getClass());
      }

      assertThat(dateBack).isEqualTo(targetDate);
    }
  }
}
