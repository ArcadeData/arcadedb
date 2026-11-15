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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11956: a DATE parameter sent over gRPC ({@code timestamp_value} tagged {@code date}) reached the engine as an
 * instant. SQL coerced it against a DATE property, but an openCypher equality on a DATE property never matched it, an
 * undeclared property stored it as a DATETIME, and {@code RETURN $p} sent it back tagged {@code datetime}.
 */
class Issue11956GrpcDateParameterIT extends BaseGrpcServerTest {
  private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

  private ManagedChannel                                channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setUp() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE D11956");
    database.command("sql", "CREATE PROPERTY D11956.da DATE");
    database.command("sql", "CREATE PROPERTY D11956.dt DATETIME");
    database.command("sql", "INSERT INTO D11956 SET k = 1, da = '2024-02-29', dt = '2024-02-29 13:14:15'");

    final Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
    headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void cypherEqualityOnADatePropertyMatchesADateParameter() {
    assertThat(query("cypher", "MATCH (n:D11956) WHERE n.da = $p RETURN n.k AS k")).hasSize(1);
    // the SQL forms kept working, against a DATE and against a DATETIME property
    assertThat(query("sql", "SELECT k FROM D11956 WHERE da = :p")).hasSize(1);
    assertThat(query("sql", "SELECT k FROM D11956 WHERE dt >= :p")).hasSize(1);
    assertThat(query("sql", "SELECT k FROM D11956 WHERE dt < :p")).isEmpty();
  }

  @Test
  void aDateParameterIsSentBackAsADate() {
    final GrpcValue echoed = query("cypher", "RETURN $p AS p").getFirst().getPropertiesOrThrow("p");
    assertThat(echoed.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATE);
    assertThat(echoed.getTimestampValue().getSeconds()).isEqualTo(DAY.toEpochDay() * 86_400L);
  }

  @Test
  void anUndeclaredPropertyStoresADateParameterAsADate() {
    stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setLanguage("cypher")
        .setCommand("CREATE (:D11956 {k: 2, u: $p})").putParameters("p", GrpcTypeConverter.toGrpcValue(DAY)).build());
    assertStoredAsDate(2);
  }

  /** CreateRecord decodes its values with the service's own decoder, not the parameter one: both read the tag. */
  @Test
  void createRecordStoresAnUndeclaredDateAsADate() {
    stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setType("D11956")
        .setRecord(GrpcRecord.newBuilder().setType("D11956").putProperties("k", GrpcTypeConverter.toGrpcValue(3))
            .putProperties("u", GrpcTypeConverter.toGrpcValue(DAY))).build());
    assertStoredAsDate(3);
  }

  private void assertStoredAsDate(final int k) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT u, u.type() AS t FROM D11956 WHERE k = ?", k)) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("t")).isEqualTo("DATE");
      assertThat(row.<Object>getProperty("u")).isEqualTo(DAY);
    }
  }

  private List<GrpcRecord> query(final String language, final String query) {
    final ExecuteQueryResponse response = stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setLanguage(language).setQuery(query).putParameters("p", GrpcTypeConverter.toGrpcValue(DAY)).build());
    return response.getResultsCount() == 0 ? List.of() : response.getResults(0).getRecordsList();
  }
}
