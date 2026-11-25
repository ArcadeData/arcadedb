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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Temporal values over gRPC carry the {@code logical_type} tag of the java.time value they wrap, both ways: a Cypher result
 * is tagged like the same value read through SQL, and a tagged DATE parameter reaches the engine as a date. Each nested
 * class is the regression of one issue and keeps that issue's own fixture; they share the server, started with the gRPC
 * plugin and nothing else configured.
 */
public class GrpcTemporalLogicalTypeIT extends BaseGrpcServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  // Regression: #11955
  /**
   * Issue #11955: a Cypher date, time or datetime (a function result, or a DATE / DATETIME property read through Cypher) reached the
   * client as an untagged string_value, where SQL sends the same property as a tagged timestamp_value. It is now encoded as the
   * java.time value it wraps.
   */
  @Nested
  class CypherTemporalResults {
    private ManagedChannel                                  channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

    @BeforeEach
    void vertexWithADateAndADatetime() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE VERTEX TYPE T11955");
      database.command("sql", "CREATE PROPERTY T11955.da DATE");
      database.command("sql", "CREATE PROPERTY T11955.dt DATETIME");
      database.transaction(() -> database.command("sql", "INSERT INTO T11955 SET da = '2024-02-29', dt = '2024-02-29 13:14:15.123'"));

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
    void cypherTemporalsAreTaggedLikeSql() {
      final GrpcRecord sql = query("sql", "SELECT da, dt FROM T11955");
      final GrpcRecord cypher = query("cypher", "MATCH (n:T11955) RETURN n.da AS da, n.dt AS dt, date('2024-01-02') AS d, "
          + "localdatetime('2024-01-02T03:04:05') AS ldt, datetime('2024-01-02T03:04:05Z') AS zdt, localtime('10:00') AS lt, "
          + "time('10:00+01:00') AS ot");

      assertThat(cypher.getPropertiesMap().get("da")).isEqualTo(sql.getPropertiesMap().get("da"));
      assertThat(cypher.getPropertiesMap().get("dt")).isEqualTo(sql.getPropertiesMap().get("dt"));
      assertThat(cypher.getPropertiesMap().get("da").getLogicalType()).isEqualTo(GrpcTemporalCodec.DATE);
      assertThat(cypher.getPropertiesMap().get("dt").getLogicalType()).isEqualTo(GrpcTemporalCodec.DATETIME);

      assertTagged(cypher, "d", GrpcValue.KindCase.TIMESTAMP_VALUE, GrpcTemporalCodec.DATE);
      assertTagged(cypher, "ldt", GrpcValue.KindCase.TIMESTAMP_VALUE, GrpcTemporalCodec.DATETIME);
      assertTagged(cypher, "zdt", GrpcValue.KindCase.STRING_VALUE, GrpcTemporalCodec.ZONED_DATETIME);
      assertTagged(cypher, "lt", GrpcValue.KindCase.STRING_VALUE, GrpcTemporalCodec.LOCAL_TIME);
      assertTagged(cypher, "ot", GrpcValue.KindCase.STRING_VALUE, GrpcTemporalCodec.OFFSET_TIME);
      assertThat(cypher.getPropertiesMap().get("ot").getStringValue()).isEqualTo("10:00:00+01:00");
    }

    private static void assertTagged(final GrpcRecord record, final String column, final GrpcValue.KindCase kind, final String tag) {
      final GrpcValue value = record.getPropertiesMap().get(column);
      assertThat(value.getKindCase()).as(column).isEqualTo(kind);
      assertThat(value.getLogicalType()).as(column).isEqualTo(tag);
    }

    private GrpcRecord query(final String language, final String statement) {
      final ExecuteQueryResponse response = stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
          .setLanguage(language).setQuery(statement).build());
      assertThat(response.getResults(0).getRecordsCount()).isEqualTo(1);
      return response.getResults(0).getRecords(0);
    }
  }

  // Regression: #11956
  /**
   * Issue #11956: a DATE parameter sent over gRPC ({@code timestamp_value} tagged {@code date}) reached the engine as an
   * instant. SQL coerced it against a DATE property, but an openCypher equality on a DATE property never matched it, an
   * undeclared property stored it as a DATETIME, and {@code RETURN $p} sent it back tagged {@code datetime}.
   */
  @Nested
  class DateParameter {
    private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

    private ManagedChannel                                channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

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

    /** arcadedb.dateImplementation=java.util.Date reads a DATE back as a java.util.Date: the date parameter still matches it (issue #9647). */
    @Test
    void cypherEqualityOnADatePropertyMatchesADateParameterUnderTheJavaUtilDateImplementation() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'").close();
      // the setting took effect: otherwise the test silently runs the default LocalDate path
      try (final ResultSet rs = database.query("sql", "SELECT da FROM D11956 WHERE k = 1")) {
        assertThat(rs.next().<Object>getProperty("da")).isInstanceOf(Date.class);
      }

      assertThat(query("cypher", "MATCH (n:D11956) WHERE n.da = $p RETURN n.k AS k")).hasSize(1);
      assertThat(query("sql", "SELECT k FROM D11956 WHERE da = :p")).hasSize(1);
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
}
