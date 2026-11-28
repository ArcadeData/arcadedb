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
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13545: a node or relationship that is a column of a row ({@code RETURN a, r, b}) was MAP-encoded with {@code @rid},
 * {@code @type} and its properties only. Without {@code @cat} a client cannot tell a nested node from a nested relationship (the
 * type of either is just a name), and without {@code @out} / {@code @in} it cannot connect a relationship to its nodes - both of
 * which the same elements carry as a top-level record and in the JSON encoding.
 */
public class Issue13545GrpcNestedElementMetadataIT extends BaseGrpcServerTest {
  private static final String STATEMENT = "MATCH (a:Admin13545)-[r:KNOWS13545]->(b) RETURN a, r, b";

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void graph() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("opencypher", "CREATE (:Person13545:Admin13545 {name: 'a'})-[:KNOWS13545 {since: 2020}]->(:Person13545 {name: 'b'})");

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
  void aNestedElementCarriesItsCategoryAndAnEdgeItsEndpoints() {
    final GrpcRecord row = command();

    final Map<String, GrpcValue> a = map(row, "a");
    final Map<String, GrpcValue> r = map(row, "r");
    final Map<String, GrpcValue> b = map(row, "b");

    assertThat(a.get("@cat").getStringValue()).isEqualTo("v");
    assertThat(b.get("@cat").getStringValue()).isEqualTo("v");
    assertThat(r.get("@cat").getStringValue()).isEqualTo("e");
    assertThat(a).doesNotContainKeys("@out", "@in");
    assertThat(b).doesNotContainKeys("@out", "@in");

    assertThat(r.get("@out").getLinkValue().getRid()).isEqualTo(a.get("@rid").getLinkValue().getRid());
    assertThat(r.get("@in").getLinkValue().getRid()).isEqualTo(b.get("@rid").getLinkValue().getRid());

    // what it carried before is still there
    assertThat(a.get("name").getStringValue()).isEqualTo("a");
    assertThat(r).containsKey("since");
    assertThat(r.get("@type").getStringValue()).isEqualTo("KNOWS13545");
  }

  private GrpcRecord command() {
    final ExecuteCommandResponse response = stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setLanguage("opencypher").setCommand(STATEMENT).setReturnRows(true)
        .setProjectionSettings(ProjectionSettings.newBuilder().setIncludeProjections(true)
            .setProjectionEncoding(ProjectionEncoding.PROJECTION_AS_MAP).build())
        .build());
    assertThat(response.getRecordsCount()).isEqualTo(1);
    return response.getRecords(0);
  }

  private static Map<String, GrpcValue> map(final GrpcRecord row, final String column) {
    final GrpcValue value = row.getPropertiesMap().get(column);
    assertThat(value.getKindCase()).as(column).isEqualTo(GrpcValue.KindCase.MAP_VALUE);
    return value.getMapValue().getEntriesMap();
  }
}
