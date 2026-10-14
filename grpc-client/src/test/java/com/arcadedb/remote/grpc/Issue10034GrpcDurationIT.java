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
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10034, end to end: a {@code DURATION} read over gRPC arrives as a {@link CypherDuration} instead of an untagged
 * string, can be written back as a predicate that matches it, and is stored typed when a client writes it. The read path is
 * {@code ArcadeDbGrpcService}'s own encoder, the write path its {@code convertWithSchemaType}, and the parameter path its
 * {@code fromGrpcValue}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10034GrpcDurationIT extends BaseGrpcClientServerTest {

  private static final String TYPE = "Duration10034";

  private static final CypherDuration CALENDAR_AND_CLOCK  = new CypherDuration(14, 3, 3723, 4_500_000);
  private static final CypherDuration NEGATIVE_WITH_NANOS = new CypherDuration(0, 0, -61, 500_000_000);

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
    grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.d IF NOT EXISTS DURATION");
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
  void storedDurationsReadBackOverGrpcTyped() {
    final Database server = getServerDatabase(0, getDatabaseName());
    server.transaction(() -> {
      server.newVertex(TYPE).set("k", "mixed").set("d", CALENDAR_AND_CLOCK).save();
      server.newVertex(TYPE).set("k", "negative").set("d", NEGATIVE_WITH_NANOS).save();
    });

    try (final ResultSet rs = grpc.query("sql", "SELECT d FROM `" + TYPE + "` WHERE k = 'mixed'")) {
      assertThat(rs.next().<Object>getProperty("d")).isInstanceOf(CypherDuration.class).isEqualTo(CALENDAR_AND_CLOCK);
    }
    try (final ResultSet rs = grpc.query("sql", "SELECT d FROM `" + TYPE + "` WHERE k = 'negative'")) {
      assertThat(rs.next().<Object>getProperty("d")).isInstanceOf(CypherDuration.class).isEqualTo(NEGATIVE_WITH_NANOS);
    }
  }

  @Test
  void aDurationComputedByCypherArrivesTyped() {
    try (final ResultSet rs = grpc.query("opencypher", "RETURN duration('P1Y2M3DT4H5M6.789S') AS d")) {
      assertThat(rs.next().<Object>getProperty("d")).isInstanceOf(CypherDuration.class)
          .isEqualTo(new CypherDuration(14, 3, 14706, 789_000_000));
    }
  }

  @Test
  void valueReadOverGrpcMatchesAsAPredicate() {
    final Database server = getServerDatabase(0, getDatabaseName());
    server.transaction(() -> {
      server.newVertex(TYPE).set("k", "mixed").set("d", CALENDAR_AND_CLOCK).save();
      server.newVertex(TYPE).set("k", "negative").set("d", NEGATIVE_WITH_NANOS).save();
    });

    final Object readBack;
    try (final ResultSet rs = grpc.query("sql", "SELECT d FROM `" + TYPE + "` WHERE k = 'negative'")) {
      readBack = rs.next().getProperty("d");
    }

    // The value the client got is the value it can ask for: one row, the negative one
    try (final ResultSet rs = grpc.query("sql", "SELECT k FROM `" + TYPE + "` WHERE d = :d", Map.of("d", readBack))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("k")).isEqualTo("negative");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void durationsWrittenOverGrpcAreStoredTyped() {
    grpc.begin();
    final MutableVertex v = grpc.newVertex(TYPE);
    v.set("k", "written");
    v.set("d", CALENDAR_AND_CLOCK);
    v.save();
    grpc.commit();

    final Database server = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = server.query("sql", "SELECT d FROM `" + TYPE + "` WHERE k = 'written'")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("d")).isInstanceOf(CypherDuration.class).isEqualTo(CALENDAR_AND_CLOCK);
    }
  }
}
