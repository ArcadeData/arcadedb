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

import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteSchema;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7740, item 3: {@code RemoteSchema.reload()} reused a cached type instance by NAME alone, so the kind
 * switch that decides its Java class ran only for a name the previous snapshot did not carry.
 * <p>
 * A drop-and-recreate that changes a type's kind is therefore invisible to a live connection: the same name coming
 * back as a TIMESERIES type left {@code getType(name) instanceof TimeSeriesType} false forever on that connection,
 * an explicit {@code reload()} included, and the only recovery was a new {@link RemoteDatabase}. The line is
 * pre-existing; what made it consequential is that a remote type acquired a kind to change into.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7740">issue #7740</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7740RemoteSchemaReloadKindIT extends BaseGraphServerTest {

  private RemoteDatabase remote() {
    return new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  @Test
  void aTypeRecreatedAsATimeSeriesTypeComesBackAsOne() {
    try (final RemoteDatabase database = remote()) {
      database.command("sql", "CREATE DOCUMENT TYPE Metric");
      assertThat(database.getSchema().getType("Metric")).isNotInstanceOf(TimeSeriesType.class);

      database.command("sql", "DROP TYPE Metric");
      database.command("sql", "CREATE TIMESERIES TYPE Metric TIMESTAMP ts FIELDS (value DOUBLE)");
      // Explicitly, because that is the strong form of the claim: even asking for a reload did not change it.
      ((RemoteSchema) database.getSchema()).reload();

      assertThat(database.getSchema().getType("Metric"))
          .as("the kind is re-read on the connection that changed it, not only on a fresh one")
          .isInstanceOf(TimeSeriesType.class);
    }
  }

  /** The same in the other direction, and on a kind pair that predates TIMESERIES types entirely. */
  @Test
  void aTypeRecreatedAsAVertexTypeComesBackAsOne() {
    try (final RemoteDatabase database = remote()) {
      database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts FIELDS (value DOUBLE)");
      assertThat(database.getSchema().getType("Reading")).isInstanceOf(TimeSeriesType.class);

      database.command("sql", "DROP TYPE Reading");
      database.command("sql", "CREATE VERTEX TYPE Reading");
      ((RemoteSchema) database.getSchema()).reload();

      final DocumentType recreated = database.getSchema().getType("Reading");
      assertThat(recreated).isInstanceOf(VertexType.class);
      assertThat(recreated).isNotInstanceOf(TimeSeriesType.class);
    }
  }

  /** A type that did not change kind keeps being reloaded in place, which is what the cache is for. */
  @Test
  void aTypeThatKeepsItsKindIsReloadedInPlace() {
    try (final RemoteDatabase database = remote()) {
      database.command("sql", "CREATE DOCUMENT TYPE Stable");
      final DocumentType first = database.getSchema().getType("Stable");

      database.command("sql", "CREATE PROPERTY Stable.name STRING");
      ((RemoteSchema) database.getSchema()).reload();
      final DocumentType second = database.getSchema().getType("Stable");

      assertThat(second).as("same kind, same instance").isSameAs(first);
      assertThat(second.existsProperty("name")).isTrue();
    }
  }
}
