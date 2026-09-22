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
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesQueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7675, the gRPC half of the projection row: {@code TimeSeriesQuery.fields} shares
 * {@code TimeSeriesGateway}'s resolver with both HTTP endpoints, so the silent drop the HTTP endpoints were
 * reported for was the gRPC behaviour too - and refusing it in the shared resolver has to land on this RPC.
 * <p>
 * The two are driven through the SAME client call here, once over each protocol, because that is the claim the
 * issue is actually about: not that each surface refuses, but that they refuse the SAME input. A test that
 * drove only gRPC would pass against a fix that refused on gRPC and dropped on HTTP, which is the state this
 * issue exists to end.
 * <p>
 * {@code bucket_interval_ms <= 0} and an empty {@code requests} list are NOT driven from here: this client's
 * {@code TimeSeriesQuery.aggregate} refuses both before a request is built, so the wire cannot be reached
 * through it. The server-side rule those two share with HTTP is pinned in
 * {@code Issue7675EdgeCaseRequestContractTest}, and the status this RPC answers is unchanged - the refusal
 * moved from an inline {@code Status.INVALID_ARGUMENT} to an {@code IllegalArgumentException} that
 * {@code GrpcErrorMapper} classifies to the same code.
 */
class Issue7675GrpcTimeSeriesProjectionIT extends BaseGrpcClientServerTest {

  private static final String TYPE      = "projectionweather";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RemoteDatabase     http;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (grpc != null) {
      grpc.close();
      grpc = null;
    }
    if (http != null) {
      http.close();
      http = null;
    }
    if (grpcServer != null) {
      grpcServer.close();
      grpcServer = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /**
   * {@code hasStackTraceContaining} rather than {@code hasMessageContaining}, because the two clients carry the
   * server's sentence in different places: {@code RemoteGrpcDatabase} puts the RPC status description in the
   * exception's own message, while {@code RemoteDatabase.postToTimeSeriesEndpoint} raises a fixed
   * "Error on time series query" and hangs the server's {@code error} body off it as the CAUSE. What this test
   * is about is that the refusal reaches the caller on both, so it asserts over the whole chain; the asymmetry
   * itself is a client-side wart and is tracked separately.
   */
  @Test
  void anUnresolvableProjectionNameIsRefusedOverGrpcAndOverHttpAlike() {
    createTypeWithSamples();

    assertThatThrownBy(() -> grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE).fields("temprature")))
        .as("gRPC used to answer a timestamp-only row for a typo, because it shares the dropping resolver")
        .hasStackTraceContaining("temprature");

    assertThatThrownBy(() -> httpClient().timeSeriesQuery(new TimeSeriesQuery(TYPE).fields("temprature")))
        .as("and the same request over HTTP has to be refused the same way")
        .hasStackTraceContaining("temprature");
  }

  /**
   * The wholly-unresolvable projection is the one that WIDENED rather than narrowed: every name was dropped,
   * the index array came back empty, and the engine reads an empty projection as "every column". A caller that
   * mistyped its whole projection was answered the full row.
   */
  @Test
  void aProjectionWhereNoNameResolvesIsRefusedRatherThanWidenedToEveryColumn() {
    createTypeWithSamples();

    assertThatThrownBy(() -> grpcClient().timeSeriesQuery(new TimeSeriesQuery(TYPE).fields("nope", "alsonope")))
        .hasStackTraceContaining("nope");
  }

  /**
   * The counter-case that keeps the refusal honest: a projection that resolves still answers exactly the
   * requested column, over both protocols and with the same columns.
   */
  @Test
  void aResolvableProjectionStillAnswersTheSameColumnsOverBothProtocols() {
    createTypeWithSamples();

    final TimeSeriesQueryResult overGrpc = grpcClient()
        .timeSeriesQuery(new TimeSeriesQuery(TYPE).fields("temperature"));
    final TimeSeriesQueryResult overHttp = httpClient()
        .timeSeriesQuery(new TimeSeriesQuery(TYPE).fields("temperature"));

    assertThat(overGrpc.columns()).containsExactly("ts", "temperature");
    assertThat(overHttp.columns()).isEqualTo(overGrpc.columns());
    assertThat(overGrpc.rows()).hasSize(3);
    assertThat(overHttp.rows()).hasSameSizeAs(overGrpc.rows());
  }

  private int httpPort() {
    return getServer(0).getHttpServer().getPort();
  }

  private RemoteGrpcDatabase grpcClient() {
    if (grpc == null) {
      grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
      grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), httpPort(), getDatabaseName(), "root",
          DEFAULT_PASSWORD_FOR_TESTS);
    }
    return grpc;
  }

  private RemoteDatabase httpClient() {
    if (http == null)
      http = new RemoteDatabase("127.0.0.1", httpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    return http;
  }

  private void createTypeWithSamples() {
    grpcClient().command("sql", "CREATE TIMESERIES TYPE " + TYPE
        + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
    grpcClient().timeSeriesWrite(List.of(
        new TimeSeriesPoint(TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 22.5)),
        new TimeSeriesPoint(TYPE, 2_000L, Map.of("location", "us-west"), Map.of("temperature", 18.3)),
        new TimeSeriesPoint(TYPE, 3_000L, Map.of("location", "us-east"), Map.of("temperature", 23.1))));
  }
}
