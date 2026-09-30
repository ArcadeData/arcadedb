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
package com.arcadedb.remote;

import com.arcadedb.schema.MaterializedViewRefreshMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #7688: the remote view builders invalidate the schema's type cache even when the CREATE command fails, because a
 * command the server applied but whose response was lost leaves the backing type on the server, and a cache left
 * untouched would answer {@code existsType()} with false for it (code review on PR #8727 asked for this to be pinned).
 */
class Issue7688RemoteViewBuilderCacheInvalidationTest {

  private RemoteDatabase database;
  private RemoteSchema   schema;

  @BeforeEach
  void setUp() {
    database = mock(RemoteDatabase.class);
    schema = spy(new RemoteSchema(database));
  }

  @Test
  void aMaterializedViewCreateThatFailsOnTheWireStillInvalidatesTheTypeCache() {
    when(database.command(eq("sql"), startsWith("CREATE MATERIALIZED VIEW"))).thenThrow(new RemoteException("connection lost"));

    assertThatThrownBy(() -> schema.buildMaterializedView().withName("V").withQuery("SELECT FROM Account")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create())
        .isInstanceOf(RemoteException.class).hasMessageContaining("connection lost");

    verify(schema).invalidateSchema();
  }

  @Test
  void aContinuousAggregateCreateThatFailsOnTheWireStillInvalidatesTheTypeCache() {
    when(database.command(eq("sql"), startsWith("CREATE CONTINUOUS AGGREGATE"))).thenThrow(new RemoteException("connection lost"));

    assertThatThrownBy(() -> schema.buildContinuousAggregate().withName("A")
        .withQuery("SELECT ts.timeBucket('1h', ts) AS hour, count(*) AS c FROM Readings GROUP BY hour").create())
        .isInstanceOf(RemoteException.class).hasMessageContaining("connection lost");

    verify(schema).invalidateSchema();
  }
}
