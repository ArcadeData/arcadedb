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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8261: the portal's SET marker is cleared only AFTER the SET applied (issue #8135), so a refused SET leaves
 * it standing and a replay of the same portal is refused again instead of answering {@code CommandComplete SET}
 * having applied nothing. The wire can no longer reach a replay of the same portal (see
 * {@code Issue8135SetAppliedAtExecuteIT}), so the ordering is pinned here, on the method itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresApplyPendingSettingTest {

  @Test
  void refusedSetLeavesTheMarkerSoAReplayIsRefusedAgain() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    final PostgresPortal portal = new PostgresPortal("SET server_version = '1'", "sql");
    final PostgresSessionSettings.Assignment refused = new PostgresSessionSettings.Assignment("server_version", "1", false);
    portal.setting = refused;

    assertThatThrownBy(() -> PostgresNetworkExecutor.applyPendingSetting(settings, portal)).hasMessageContaining("cannot be changed");
    assertThat(portal.setting).as("a refused SET must not consume the marker").isSameAs(refused);

    assertThatThrownBy(() -> PostgresNetworkExecutor.applyPendingSetting(settings, portal))
        .as("the replay reaches the SET again and is refused again")
        .hasMessageContaining("cannot be changed");
  }

  @Test
  void appliedSetClearsTheMarkerSoAReplayDoesNotApplyItAgain() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    final PostgresPortal portal = new PostgresPortal("SET application_name = 'first'", "sql");
    portal.setting = new PostgresSessionSettings.Assignment("application_name", "first", false);

    PostgresNetworkExecutor.applyPendingSetting(settings, portal);
    assertThat(portal.setting).isNull();
    assertThat(settings.show("application_name")).isEqualTo("first");

    settings.set("application_name", "second");
    PostgresNetworkExecutor.applyPendingSetting(settings, portal);
    assertThat(settings.show("application_name")).as("an already applied portal must not apply its SET again").isEqualTo("second");
  }
}
