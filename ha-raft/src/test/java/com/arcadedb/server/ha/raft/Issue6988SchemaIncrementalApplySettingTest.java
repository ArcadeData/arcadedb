/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6988: {@code arcadedb.ha.schemaIncrementalApply} is the escape hatch that sends a follower's schema apply
 * back through the full {@code LocalSchema.load()}. Its whole value to an operator is that it takes effect on the
 * NEXT applied entry rather than on the next restart, which means the value must be read per entry and never cached.
 * <p>
 * {@code Issue6988FullRebuildFallbackIT} proves the flag reaches the apply path, but it sets the value once at server
 * start, so it cannot tell a per-entry read from a value latched during startup. This test pins that half.
 */
class Issue6988SchemaIncrementalApplySettingTest {

  @AfterEach
  void restoreDefault() {
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.reset();
  }

  @Test
  void defaultsToEnabled() {
    assertThat(GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.getDefValue()).isEqualTo(true);
    assertThat(GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.getType()).isEqualTo(Boolean.class);
    assertThat(new ArcadeStateMachine().incrementalSchemaApplyEnabled()).isTrue();
  }

  @Test
  void theSettingIsReReadOnEveryEntryRatherThanLatchedAtStartup() {
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
    assertThat(stateMachine.incrementalSchemaApplyEnabled()).isTrue();

    // The same instance that already answered "enabled" must answer "disabled" the moment the value changes: an
    // operator turning the valve on a running server expects the very next applied entry to take the full rebuild.
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);
    assertThat(stateMachine.incrementalSchemaApplyEnabled()).isFalse();

    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(true);
    assertThat(stateMachine.incrementalSchemaApplyEnabled()).isTrue();
  }
}
