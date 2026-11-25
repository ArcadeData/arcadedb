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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11959: ExecuteCommand ignored the request's projection settings and always sent a nested record as JSON bytes, where
 * ExecuteQuery and StreamQuery honour them. The three RPCs now encode the same statement alike.
 */
public class Issue11959GrpcCommandProjectionIT extends BaseGrpcServerTest {
  private static final String VERTEX    = "V11959";
  private static final String EDGE      = "E11959";
  private static final String STATEMENT = "SELECT k, outE('" + EDGE + "') AS oe FROM " + VERTEX + " WHERE k = 1";

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
  private String                                          ridEdge;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void graphOfTwoVerticesAndOneEdge() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE " + VERTEX);
    database.command("sql", "CREATE EDGE TYPE " + EDGE);
    database.transaction(() -> {
      final MutableVertex a = database.newVertex(VERTEX).set("k", 1).save();
      final MutableVertex b = database.newVertex(VERTEX).set("k", 2).save();
      ridEdge = a.newEdge(EDGE, b, "w", 1.5).getIdentity().toString();
    });

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
    headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
    stub = ArcadeDbServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
  }

  @AfterEach
  void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void commandHonoursTheMapEncoding() {
    final GrpcValue edge = firstEdge(command(projection(ProjectionEncoding.PROJECTION_AS_MAP)));
    assertThat(edge.getKindCase()).isEqualTo(GrpcValue.KindCase.MAP_VALUE);
    assertThat(edge.getMapValue().getEntriesMap().get("@rid").getLinkValue().getRid()).isEqualTo(ridEdge);
    assertThat(edge.getMapValue().getEntriesMap().get("w").getDoubleValue()).isEqualTo(1.5);
    assertThat(edge).isEqualTo(firstEdge(query(projection(ProjectionEncoding.PROJECTION_AS_MAP))));
  }

  @Test
  void commandHonoursTheLinkEncoding() {
    final GrpcValue edge = firstEdge(command(projection(ProjectionEncoding.PROJECTION_AS_LINK)));
    assertThat(edge.getKindCase()).isEqualTo(GrpcValue.KindCase.LINK_VALUE);
    assertThat(edge.getLinkValue().getRid()).isEqualTo(ridEdge);
  }

  @Test
  void withoutSettingsCommandAndQueryKeepTheJsonDefault() {
    final GrpcValue edge = firstEdge(command(null));
    assertThat(edge.getKindCase()).isEqualTo(GrpcValue.KindCase.BYTES_VALUE);
    assertThat(edge.getLogicalType()).isEqualTo("json");
    assertThat(firstEdge(query(null)).getKindCase()).isEqualTo(GrpcValue.KindCase.BYTES_VALUE);
  }

  private GrpcRecord command(final ProjectionSettings settings) {
    final ExecuteCommandRequest.Builder request = ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setCommand(STATEMENT).setReturnRows(true);
    if (settings != null)
      request.setProjectionSettings(settings);
    final ExecuteCommandResponse response = stub.executeCommand(request.build());
    assertThat(response.getRecordsCount()).isEqualTo(1);
    return response.getRecords(0);
  }

  private GrpcRecord query(final ProjectionSettings settings) {
    final ExecuteQueryRequest.Builder request = ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setQuery(STATEMENT);
    if (settings != null)
      request.setProjectionSettings(settings);
    final ExecuteQueryResponse response = stub.executeQuery(request.build());
    assertThat(response.getResults(0).getRecordsCount()).isEqualTo(1);
    return response.getResults(0).getRecords(0);
  }

  private static GrpcValue firstEdge(final GrpcRecord record) {
    final GrpcValue list = record.getPropertiesMap().get("oe");
    assertThat(list.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    return list.getListValue().getValues(0);
  }

  private static ProjectionSettings projection(final ProjectionEncoding encoding) {
    return ProjectionSettings.newBuilder().setIncludeProjections(true).setProjectionEncoding(encoding).build();
  }
}
