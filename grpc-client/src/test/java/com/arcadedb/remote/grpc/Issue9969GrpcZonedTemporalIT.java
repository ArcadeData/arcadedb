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
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9969, end to end: a {@code ZONED_DATETIME} read over gRPC arrives with its zone, so it equals the stored value and
 * can be written back as a predicate that matches it; an {@code OFFSET_TIME} and a {@code LOCAL_TIME} arrive typed. The
 * read path is {@code ArcadeDbGrpcService}'s own encoder, the write path its {@code convertWithSchemaType}, and the parameter
 * path its {@code fromGrpcValue}.
 */
public class Issue9969GrpcZonedTemporalIT extends BaseGrpcClientServerTest {

  private static final String TYPE = "Temporal9969";

  // Same instant, two zones: the engine keeps them apart, so the wire has to as well
  private static final ZonedDateTime ROME      = ZonedDateTime.of(2026, 1, 1, 0, 30, 0, 0, ZoneId.of("Europe/Rome"));
  private static final ZonedDateTime UTC_TWIN  = ROME.withZoneSameInstant(ZoneOffset.UTC);
  private static final OffsetTime    OFFSET_T  = OffsetTime.of(0, 30, 0, 0, ZoneOffset.ofHours(2));
  private static final LocalTime     LOCAL_T   = LocalTime.of(0, 30, 0, 123_000_000);

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.z IF NOT EXISTS ZONED_DATETIME");
    grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.ot IF NOT EXISTS OFFSET_TIME");
    grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.lt IF NOT EXISTS LOCAL_TIME");
    grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.dt IF NOT EXISTS DATETIME");
    grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.dtNanos IF NOT EXISTS DATETIME_NANOS");
    grpc.command("sql", "DELETE FROM `" + TYPE + "`");
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
  void storedValuesReadBackOverGrpcWithZoneAndType() {
    final Database server = getServerDatabase(0, getDatabaseName());
    server.transaction(() -> {
      server.newVertex(TYPE).set("k", "rome").set("z", ROME).set("ot", OFFSET_T).set("lt", LOCAL_T).save();
      server.newVertex(TYPE).set("k", "utc").set("z", UTC_TWIN).save();
    });

    try (final ResultSet rs = grpc.query("sql", "SELECT z, ot, lt FROM `" + TYPE + "` WHERE k = 'rome'")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("z")).isInstanceOf(ZonedDateTime.class).isEqualTo(ROME);
      assertThat(row.<Object>getProperty("ot")).isInstanceOf(OffsetTime.class).isEqualTo(OFFSET_T);
      assertThat(row.<Object>getProperty("lt")).isInstanceOf(LocalTime.class).isEqualTo(LOCAL_T);
    }
  }

  @Test
  void valueReadOverGrpcMatchesAsAPredicate() {
    final Database server = getServerDatabase(0, getDatabaseName());
    server.transaction(() -> {
      server.newVertex(TYPE).set("k", "rome").set("z", ROME).save();
      server.newVertex(TYPE).set("k", "utc").set("z", UTC_TWIN).save();
    });

    final Object readBack;
    try (final ResultSet rs = grpc.query("sql", "SELECT z FROM `" + TYPE + "` WHERE k = 'rome'")) {
      readBack = rs.next().getProperty("z");
    }

    // The value the client got is the value it can ask for: one row, the Rome one, not its UTC twin
    try (final ResultSet rs = grpc.query("sql", "SELECT k FROM `" + TYPE + "` WHERE z = :z", Map.of("z", readBack))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("k")).isEqualTo("rome");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void valuesWrittenOverGrpcKeepZoneAndType() {
    grpc.begin();
    final MutableVertex v = grpc.newVertex(TYPE);
    v.set("k", "written");
    v.set("z", ROME);
    v.set("ot", OFFSET_T);
    v.set("lt", LOCAL_T);
    v.save();
    grpc.commit();

    final Database server = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = server.query("sql", "SELECT z, ot, lt FROM `" + TYPE + "` WHERE k = 'written'")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("z")).isEqualTo(ROME);
      assertThat(row.<Object>getProperty("ot")).isEqualTo(OFFSET_T);
      assertThat(row.<Object>getProperty("lt")).isEqualTo(LOCAL_T);
    }
  }

  @Test
  void zonedDateTimeWrittenIntoADatetimeKeepsTheExactInstant() {
    // A ZonedDateTime used to travel as a Timestamp, which a DATETIME stored exactly. Now it travels as zoned text, which must
    // not go through the string chain that rebases an offset onto the database's zone. The zone is pinned away from both the
    // value's offset and UTC so the run does not depend on the JVM's default zone
    final ZonedDateTime farOffset = ZonedDateTime.of(2026, 3, 4, 5, 6, 7, 123_456_789, ZoneOffset.ofHours(-7));

    final Database server = getServerDatabase(0, getDatabaseName());
    final ZoneId previous = server.getSchema().getZoneId();
    server.getSchema().setZoneId(ZoneId.of("Asia/Tokyo"));
    try {
      grpc.begin();
      final MutableVertex v = grpc.newVertex(TYPE);
      v.set("k", "dt");
      v.set("dt", farOffset);
      v.set("dtNanos", farOffset);
      v.save();
      grpc.commit();

      try (final ResultSet rs = server.query("sql", "SELECT dt, dtNanos FROM `" + TYPE + "` WHERE k = 'dt'")) {
        final Result row = rs.next();
        assertThat(toInstant(row.getProperty("dt"))).isEqualTo(farOffset.toInstant().truncatedTo(ChronoUnit.MILLIS));
        assertThat(toInstant(row.getProperty("dtNanos"))).isEqualTo(farOffset.toInstant());
      }
    } finally {
      server.getSchema().setZoneId(previous);
    }
  }

  private static Instant toInstant(final Object stored) {
    return stored instanceof Date d ? d.toInstant() : ((LocalDateTime) stored).toInstant(ZoneOffset.UTC);
  }
}
