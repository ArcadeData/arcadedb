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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9861 (discussion #9856): the gRPC client stopped working on any database with more than
 * 1000 types. {@code RemoteSchema.reload()} reads the schema with {@code select from schema:types} through
 * {@code command()}, and {@code RemoteGrpcDatabase.commandInternal()} sends no {@code max_rows}, so the server applied
 * a hard-coded 1000-row cap to {@code ExecuteCommand}. Since #6756 an over-cap result fails with
 * {@code RESOURCE_EXHAUSTED} ("ExecuteCommand result exceeds the maximum of 1000 rows") instead of being silently
 * truncated, and the documented {@code arcadedb.server.grpcQueryMaxResultRows} knob (default 100000) only bounded
 * {@code ExecuteQuery}, so raising it did nothing for the call that failed.
 *
 * <p>The fix makes {@code ExecuteCommand} bound its result by the same configured ceiling as {@code ExecuteQuery}: the
 * default when the request carries no {@code max_rows}, and a limit a larger {@code max_rows} cannot bypass.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9861GrpcCommandRowCeilingIT extends BaseGrpcClientServerTest {

  private static final String ROWS_TYPE             = "Issue9861Rows";
  private static final String SCHEMA_TYPE           = "Issue9861Type";
  private static final int    ROWS                  = 1200;
  private static final int    TYPES_OVER_LEGACY_CAP = 1100;

  private RemoteGrpcServer grpcServer;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void createServerHandle() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterEach
  @Override
  public void endTest() {
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private RemoteGrpcDatabase connect() {
    return new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  private void setCeiling(final int rows) {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_GRPC_QUERY_MAX_RESULT_ROWS, rows);
  }

  private void createRows() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.getSchema().createDocumentType(ROWS_TYPE);
    db.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        db.newDocument(ROWS_TYPE).set("k", i).save();
    });
  }

  private static int count(final ResultSet rs) {
    int n = 0;
    while (rs.hasNext()) {
      rs.next();
      n++;
    }
    return n;
  }

  /**
   * The reporter's scenario: a fresh connection whose first query needs the remote schema, on a database with more
   * types than the legacy hard-coded command cap, under the DEFAULT configuration.
   */
  @Test
  void schemaReloadSucceedsOnADatabaseWithMoreTypesThanTheLegacyCommandCap() {
    final Database db = getServerDatabase(0, getDatabaseName());
    for (int i = 0; i < TYPES_OVER_LEGACY_CAP; i++)
      db.getSchema().createDocumentType(SCHEMA_TYPE + i);
    db.transaction(() -> db.newDocument(SCHEMA_TYPE + 0).set("k", 1).save());

    try (RemoteGrpcDatabase remote = connect()) {
      // Before the fix: RemoteException "ExecuteCommand result exceeds the maximum of 1000 rows" from RemoteSchema.reload()
      try (ResultSet rs = remote.query("sql", "SELECT FROM " + SCHEMA_TYPE + 0)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Integer>getProperty("k")).isEqualTo(1);
      }

      assertThat(remote.getSchema().existsType(SCHEMA_TYPE + (TYPES_OVER_LEGACY_CAP - 1))).isTrue();
    }
  }

  @Test
  void commandReturnsMoreRowsThanTheLegacyCapWhenTheConfiguredCeilingAllowsIt() {
    createRows();
    setCeiling(ROWS + 100);

    try (RemoteGrpcDatabase remote = connect(); ResultSet rs = remote.command("sql", "SELECT FROM " + ROWS_TYPE)) {
      assertThat(count(rs)).isEqualTo(ROWS);
    }
  }

  @Test
  void commandFailsLoudlyOnceTheResultExceedsTheConfiguredCeiling() {
    createRows();
    setCeiling(ROWS - 100);

    try (RemoteGrpcDatabase remote = connect()) {
      assertThatThrownBy(() -> remote.command("sql", "SELECT FROM " + ROWS_TYPE)).isInstanceOf(RemoteException.class)
          .hasMessageContaining("maximum of " + (ROWS - 100) + " rows");
    }
  }

  @Test
  void explicitMaxRowsCannotBypassTheConfiguredCeiling() {
    createRows();
    setCeiling(ROWS - 100);

    try (RemoteGrpcDatabase remote = connect()) {
      assertThatThrownBy(() -> remote.executeCommand("sql", "SELECT FROM " + ROWS_TYPE, Map.of(), true, Integer.MAX_VALUE, null, 30_000L))
          .isInstanceOf(RemoteException.class).hasMessageContaining("maximum of " + (ROWS - 100) + " rows");
    }
  }

  @Test
  void explicitMaxRowsBelowTheCeilingIsHonored() {
    createRows();
    setCeiling(ROWS + 100);

    try (RemoteGrpcDatabase remote = connect()) {
      assertThatThrownBy(() -> remote.executeCommand("sql", "SELECT FROM " + ROWS_TYPE, Map.of(), true, 50, null, 30_000L))
          .isInstanceOf(RemoteException.class).hasMessageContaining("maximum of 50 rows");

      assertThat(remote.executeCommand("sql", "SELECT FROM " + ROWS_TYPE, Map.of(), true, ROWS + 50, null, 30_000L).getRecordsCount())
          .isEqualTo(ROWS);
    }
  }

  @Test
  void commandIsUnboundedWhenTheCeilingIsDisabled() {
    createRows();
    setCeiling(0);

    try (RemoteGrpcDatabase remote = connect(); ResultSet rs = remote.command("sql", "SELECT FROM " + ROWS_TYPE)) {
      assertThat(count(rs)).isEqualTo(ROWS);
    }
  }
}
