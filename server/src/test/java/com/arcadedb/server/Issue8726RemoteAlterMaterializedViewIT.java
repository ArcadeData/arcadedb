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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.MaterializedView;
import com.arcadedb.schema.MaterializedViewRefreshMode;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8726: {@code RemoteSchema.alterMaterializedView()} used to throw {@code UnsupportedOperationException}. The
 * acceptance tests run ONE body of alter code, written against the {@link Schema} interface, against the embedded
 * database and against a {@link RemoteDatabase}, and compare the two views attribute by attribute.
 */
class Issue8726RemoteAlterMaterializedViewIT extends BaseGraphServerTest {

  /** The server's OWN database instance: the fixture's separate handle is closed before the servers start. */
  private Database embedded() {
    return getServerDatabase(0, getDatabaseName());
  }

  /** The port the test server actually bound, never a hand-picked one. */
  private RemoteDatabase remote() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  @BeforeEach
  void createSourceTypeAndViews() {
    final Database database = embedded();
    if (!database.getSchema().existsType("Account"))
      database.transaction(() -> {
        database.getSchema().createDocumentType("Account");
        database.newDocument("Account").set("name", "Alice").save();
        database.newDocument("Account").set("name", "Bob").save();
      });
    for (final String name : new String[] { "ViewEmbedded", "ViewRemote" })
      if (!database.getSchema().existsMaterializedView(name))
        database.getSchema().buildMaterializedView().withName(name).withQuery("SELECT name FROM Account")
            .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create();
  }

  private static void assertSameView(final MaterializedView expected, final MaterializedView actual) {
    assertThat(actual.getRefreshMode()).isEqualTo(expected.getRefreshMode());
    assertThat(actual.getRefreshInterval()).isEqualTo(expected.getRefreshInterval());
    assertThat(actual.getSourceTypeNames()).isEqualTo(expected.getSourceTypeNames());
    assertThat(actual.getStatus()).isEqualTo(expected.getStatus());
  }

  @Test
  void theSameAlterCodeProducesTheSameViewEmbeddedAndRemotely() {
    try (final RemoteDatabase database = remote()) {
      final Schema remoteSchema = database.getSchema();
      final Schema embeddedSchema = embedded().getSchema();

      // PERIODIC: the interval reaches the server through the largest exact unit (90 minutes, not 5400 seconds)
      embeddedSchema.alterMaterializedView("ViewEmbedded", MaterializedViewRefreshMode.PERIODIC, 90L * 60_000L);
      remoteSchema.alterMaterializedView("ViewRemote", MaterializedViewRefreshMode.PERIODIC, 90L * 60_000L);
      assertThat(embeddedSchema.getMaterializedView("ViewRemote").getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.PERIODIC);
      assertThat(embeddedSchema.getMaterializedView("ViewRemote").getRefreshInterval()).isEqualTo(90L * 60_000L);
      assertSameView(embeddedSchema.getMaterializedView("ViewEmbedded"), remoteSchema.getMaterializedView("ViewRemote"));

      // INCREMENTAL: the remotely altered view follows its source afterwards, like the embedded one
      embeddedSchema.alterMaterializedView("ViewEmbedded", MaterializedViewRefreshMode.INCREMENTAL, 0);
      remoteSchema.alterMaterializedView("ViewRemote", MaterializedViewRefreshMode.INCREMENTAL, 0);
      assertThat(embeddedSchema.getMaterializedView("ViewRemote").getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.INCREMENTAL);
      assertThat(embeddedSchema.getMaterializedView("ViewRemote").getRefreshInterval()).isZero();
      assertSameView(embeddedSchema.getMaterializedView("ViewEmbedded"), remoteSchema.getMaterializedView("ViewRemote"));

      embeddedSchema.getMaterializedView("ViewEmbedded").refresh();
      embeddedSchema.getMaterializedView("ViewRemote").refresh();
      embedded().transaction(() -> embedded().newDocument("Account").set("name", "Carol").save());
      assertThat(embedded().countType("ViewRemote", false)).isEqualTo(embedded().countType("ViewEmbedded", false)).isEqualTo(3);

      // MANUAL: back to no automatic refresh
      embeddedSchema.alterMaterializedView("ViewEmbedded", MaterializedViewRefreshMode.MANUAL, 0);
      remoteSchema.alterMaterializedView("ViewRemote", MaterializedViewRefreshMode.MANUAL, 0);
      assertThat(embeddedSchema.getMaterializedView("ViewRemote").getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.MANUAL);
      assertSameView(embeddedSchema.getMaterializedView("ViewEmbedded"), remoteSchema.getMaterializedView("ViewRemote"));
    }
  }

  @Test
  void anUnknownViewIsRefusedOnBothPathsWithTheSameExceptionType() {
    assertThatThrownBy(() -> embedded().getSchema().alterMaterializedView("Missing", MaterializedViewRefreshMode.MANUAL, 0))
        .isInstanceOf(SchemaException.class).hasMessageContaining("Materialized view 'Missing' not found");

    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> database.getSchema().alterMaterializedView("Missing", MaterializedViewRefreshMode.MANUAL, 0))
          .isInstanceOf(SchemaException.class).hasMessageContaining("Materialized view 'Missing' not found");
    }
  }

  @Test
  void anIntervalOnANonPeriodicModeIsRefusedOnBothPaths() {
    assertThatThrownBy(() -> embedded().getSchema().alterMaterializedView("ViewEmbedded", MaterializedViewRefreshMode.MANUAL, 5_000L))
        .isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");

    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> database.getSchema().alterMaterializedView("ViewRemote", MaterializedViewRefreshMode.MANUAL, 5_000L))
          .isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");
    }

    assertThat(embedded().getSchema().getMaterializedView("ViewEmbedded").getRefreshInterval()).isZero();
    assertThat(embedded().getSchema().getMaterializedView("ViewRemote").getRefreshInterval()).isZero();
  }
}
