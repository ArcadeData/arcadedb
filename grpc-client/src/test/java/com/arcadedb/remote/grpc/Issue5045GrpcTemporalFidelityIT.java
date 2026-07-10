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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5045 (COR-5 / COR-6): temporal values must round-trip over gRPC as
 * temporal types with their precision intact, instead of decoding to a bare {@code Long}
 * epoch-millis (losing sub-millisecond precision and the temporal type identity, and forcing the
 * caller to re-apply a timezone).
 * <p>
 * The client {@code ProtoUtils.fromGrpcValue} now reconstructs the temporal type from the
 * {@code logical_type} tag the server sets, symmetrically with the UTC-anchored encode side.
 */
public class Issue5045GrpcTemporalFidelityIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final int    HTTP_PORT   = 2480;
  private static final String VERTEX_TYPE = "Temporal5045";

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
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.d IF NOT EXISTS DATE");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.dt IF NOT EXISTS DATETIME");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.dtMicros IF NOT EXISTS DATETIME_MICROS");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.dtNanos IF NOT EXISTS DATETIME_NANOS");
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
  void dateRoundTripsAsLocalDate() {
    final LocalDate target = LocalDate.of(2026, 7, 6);

    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("d", target);
    v.save();
    grpc.commit();

    try (final ResultSet rs = grpc.query("sql", "SELECT d FROM `" + VERTEX_TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      final Object raw = rs.next().getProperty("d");
      assertThat(raw).as("DATE must decode to a LocalDate, not a Long").isInstanceOf(LocalDate.class);
      assertThat((LocalDate) raw).isEqualTo(target);
    }
  }

  @Test
  void preEpochDateRoundTripsAsLocalDate() {
    // Guards the floorDiv() path on the decode side: a pre-epoch date has a negative epoch-day,
    // so plain integer division would round toward zero and land on the wrong day.
    final LocalDate target = LocalDate.of(1969, 12, 31);

    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("d", target);
    v.save();
    grpc.commit();

    try (final ResultSet rs = grpc.query("sql", "SELECT d FROM `" + VERTEX_TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      final Object raw = rs.next().getProperty("d");
      assertThat(raw).as("pre-epoch DATE must decode to a LocalDate").isInstanceOf(LocalDate.class);
      assertThat((LocalDate) raw).isEqualTo(target);
    }
  }

  @Test
  void datetimeRoundTripsAsLocalDateTime() {
    final LocalDateTime target = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_000_000);

    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("dt", target);
    v.save();
    grpc.commit();

    try (final ResultSet rs = grpc.query("sql", "SELECT dt FROM `" + VERTEX_TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      final Object raw = rs.next().getProperty("dt");
      assertThat(raw).as("DATETIME must decode to a LocalDateTime, not a Long").isInstanceOf(LocalDateTime.class);
      assertThat((LocalDateTime) raw).isEqualTo(target);
    }
  }

  @Test
  void datetimeMicrosRoundTripsPreservingMicros() {
    final LocalDateTime target = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_000);

    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("dtMicros", target);
    v.save();
    grpc.commit();

    try (final ResultSet rs = grpc.query("sql", "SELECT dtMicros FROM `" + VERTEX_TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      final Object raw = rs.next().getProperty("dtMicros");
      assertThat(raw).as("DATETIME_MICROS must decode to a LocalDateTime").isInstanceOf(LocalDateTime.class);
      assertThat(((LocalDateTime) raw).getNano()).as("microsecond precision preserved").isEqualTo(123_456_000);
      assertThat((LocalDateTime) raw).isEqualTo(target);
    }
  }

  @Test
  void datetimeNanosRoundTripsPreservingNanos() {
    final LocalDateTime target = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789);

    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("dtNanos", target);
    v.save();
    grpc.commit();

    try (final ResultSet rs = grpc.query("sql", "SELECT dtNanos FROM `" + VERTEX_TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      final Object raw = rs.next().getProperty("dtNanos");
      assertThat(raw).as("DATETIME_NANOS must decode to a LocalDateTime").isInstanceOf(LocalDateTime.class);
      assertThat(((LocalDateTime) raw).getNano()).as("nanosecond precision preserved").isEqualTo(123_456_789);
      assertThat((LocalDateTime) raw).isEqualTo(target);
    }
  }
}
