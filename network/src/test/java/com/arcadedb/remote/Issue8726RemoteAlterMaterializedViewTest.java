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

import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.MaterializedViewRefreshMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Issue #8726: {@code RemoteSchema.alterMaterializedView()} used to throw {@code UnsupportedOperationException} although
 * {@code ALTER MATERIALIZED VIEW} expresses it. It now renders the call as that statement, and refuses client-side -
 * without sending anything - every argument the grammar has no expression for.
 */
class Issue8726RemoteAlterMaterializedViewTest {

  private RemoteDatabase database;
  private RemoteSchema   schema;

  @BeforeEach
  void setUp() {
    database = mock(RemoteDatabase.class);
    schema = new RemoteSchema(database);
  }

  @Test
  void manualRendersRefreshManual() {
    schema.alterMaterializedView("View", MaterializedViewRefreshMode.MANUAL, 0);
    verify(database).command("sql", "ALTER MATERIALIZED VIEW `View` REFRESH MANUAL");
  }

  @Test
  void incrementalRendersRefreshIncremental() {
    schema.alterMaterializedView("View", MaterializedViewRefreshMode.INCREMENTAL, 0);
    verify(database).command("sql", "ALTER MATERIALIZED VIEW `View` REFRESH INCREMENTAL");
  }

  @Test
  void periodicRendersTheLargestExactUnit() {
    schema.alterMaterializedView("View", MaterializedViewRefreshMode.PERIODIC, 2 * 3_600_000L);
    verify(database).command("sql", "ALTER MATERIALIZED VIEW `View` REFRESH EVERY 2 HOUR");

    schema.alterMaterializedView("View", MaterializedViewRefreshMode.PERIODIC, 90 * 60_000L);
    verify(database).command("sql", "ALTER MATERIALIZED VIEW `View` REFRESH EVERY 90 MINUTE");

    schema.alterMaterializedView("View", MaterializedViewRefreshMode.PERIODIC, 45_000L);
    verify(database).command("sql", "ALTER MATERIALIZED VIEW `View` REFRESH EVERY 45 SECOND");
  }

  @Test
  void theViewNameIsQuoted() {
    schema.alterMaterializedView("my`view", MaterializedViewRefreshMode.MANUAL, 0);
    verify(database).command("sql", "ALTER MATERIALIZED VIEW `my\\`view` REFRESH MANUAL");
  }

  @Test
  void anIntervalThatIsNotAWholeNumberOfSecondsIsRefusedWithoutReachingTheServer() {
    assertThatThrownBy(() -> schema.alterMaterializedView("View", MaterializedViewRefreshMode.PERIODIC, 1_500L))
        .isInstanceOf(SchemaException.class).hasMessageContaining("whole number of seconds");
    verify(database, never()).command(anyString(), anyString());
  }

  @Test
  void anIntervalOnANonPeriodicModeIsRefusedWithoutReachingTheServer() {
    assertThatThrownBy(() -> schema.alterMaterializedView("View", MaterializedViewRefreshMode.MANUAL, 5_000L))
        .isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");
    assertThatThrownBy(() -> schema.alterMaterializedView("View", MaterializedViewRefreshMode.INCREMENTAL, 5_000L))
        .isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");
    verify(database, never()).command(anyString(), anyString());
  }

  @Test
  void aNullModeANegativeIntervalAndAMissingNameAreRefusedWithoutReachingTheServer() {
    assertThatThrownBy(() -> schema.alterMaterializedView("View", null, 0))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("refresh mode is required");
    assertThatThrownBy(() -> schema.alterMaterializedView("View", MaterializedViewRefreshMode.PERIODIC, -1L))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot be negative");
    assertThatThrownBy(() -> schema.alterMaterializedView(null, MaterializedViewRefreshMode.MANUAL, 0))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("name is required");
    assertThatThrownBy(() -> schema.alterMaterializedView("", MaterializedViewRefreshMode.MANUAL, 0))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("name is required");
    verify(database, never()).command(anyString(), anyString());
  }
}
