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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6404: {@link RemoteGrpcDatabase#lookupByRID(RID)} threw {@link
 * com.arcadedb.exception.RecordNotFoundException} for an existing, previously persisted vertex, even though the
 * same RID was returned by a SQL query on the same connection and was loadable through the HTTP client.
 *
 * <p>Root cause: {@code ArcadeDbGrpcService.convertToGrpcRecord()} / {@code convertResultToGrpcRecord()} never sent
 * the {@code @cat} metadata property that {@code JsonSerializer} always sends over HTTP. The client's
 * {@code grpcRecordToDBRecord()} falls back to resolving the category through the remote schema
 * ({@code RemoteSchema.existsType()}) when {@code @cat} is absent, and a freshly opened connection - one that has
 * never otherwise touched the schema - resolves that lookup against a type the client has not (yet, or ever)
 * cached, so the fallback returns {@code null} and the "found" response is turned into a spurious
 * "not found" exception. SQL queries do not notice because {@code grpcRecordToResult()} has a property-only
 * fallback that {@code lookupByRID()}, which must return a concrete {@link Record}, does not have.
 *
 * <p>The fix sends {@code @cat} on the wire, matching HTTP, so the client never needs the schema fallback at all.
 * This is the end-to-end reproduction through the high-level client; {@code Issue6404LookupByRidMissingCatIT} in
 * {@code grpcw} pins the same fix directly against the wire response.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6404LookupByRidEndToEndIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final int    HTTP_PORT   = 2480;
  private static final String VERTEX_TYPE = "Issue6404Vertex";

  private RemoteGrpcServer grpcServer;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void createServerHandle() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterEach
  @Override
  public void endTest() {
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /**
   * Mirrors the issue's reproducer: the type and the vertex are created and persisted on one connection (standing
   * in for "a previous session"), then a BRAND NEW {@link RemoteGrpcDatabase} - whose {@code RemoteSchema} has never
   * been touched - looks the vertex up by RID found through a plain SQL query, never through the schema.
   */
  @Test
  void lookupByRidFindsAnExistingVertexOnAFreshConnectionThatNeverLoadedTheSchema() {
    final RID rid;
    try (RemoteGrpcDatabase setup = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, HTTP_PORT, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      setup.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS");
      setup.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.ldapId IF NOT EXISTS STRING");
      setup.command("sql", "DELETE FROM `" + VERTEX_TYPE + "`");
      try (ResultSet rs = setup.command("sql", "INSERT INTO `" + VERTEX_TYPE + "` SET ldapId = 'heimdall'")) {
        rid = rs.next().getIdentity().orElseThrow();
      }
    }

    try (RemoteGrpcDatabase fresh = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, HTTP_PORT, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {

      try (ResultSet rs = fresh.query("sql", "SELECT FROM `" + VERTEX_TYPE + "` WHERE ldapId = 'heimdall'")) {
        assertThat(rs.hasNext()).isTrue();
        final Result queried = rs.next();
        assertThat(queried.getIdentity().orElseThrow()).isEqualTo(rid);
      }

      // Before the fix: RecordNotFoundException, even though the row above was found through the same connection.
      final Record record = fresh.lookupByRID(rid);

      assertThat(record).isInstanceOf(Vertex.class);
      assertThat(record.asVertex().getTypeName()).isEqualTo(VERTEX_TYPE);
      assertThat(record.asVertex().getString("ldapId")).isEqualTo("heimdall");
    }
  }
}
