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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6981: the shutdown-hook WARNING (and the setting description) pointed the
 * operator at {@code arcadedb.server.shutdownTimeout} on the one path that ignores it.
 * <p>
 * While the server is {@code STARTING} the hook waits a fixed 2000ms, not the configured timeout; an
 * operator who followed the "raise arcadedb.server.shutdownTimeout" advice on that branch changed a value
 * that provably had no effect. The hint must name the setting only on the branch it governs.
 */
class Issue6981ShutdownHookHintTest {

  @Test
  void startingWarningMustNotAdviseRaisingTheSettingItIgnores() {
    final String message = ArcadeDBServer.shutdownHookLockTimeoutWarning(2_000, ArcadeDBServer.STATUS.STARTING);

    assertThat(message).contains("2000ms").contains("STARTING");
    assertThat(message)
        .as("on the STARTING branch the configured timeout is not in effect, so 'raise it' advice is misleading")
        .doesNotContain("Raise arcadedb.server.shutdownTimeout");
    assertThat(message)
        .as("the operator should learn the wait is a fixed bound the setting does not govern")
        .contains("not governed by arcadedb.server.shutdownTimeout");
  }

  @Test
  void warningOutsideStartingKeepsTheRaiseHint() {
    for (final ArcadeDBServer.STATUS status : new ArcadeDBServer.STATUS[] { ArcadeDBServer.STATUS.OFFLINE,
        ArcadeDBServer.STATUS.ONLINE, ArcadeDBServer.STATUS.SHUTTING_DOWN }) {
      final String message = ArcadeDBServer.shutdownHookLockTimeoutWarning(60_000, status);

      assertThat(message).contains("60000ms").contains(status.toString());
      assertThat(message)
          .as("on every non-STARTING branch the setting IS the bound, so the advice stays")
          .contains("Raise arcadedb.server.shutdownTimeout if a legitimate shutdown needs longer.");
    }
  }

  @Test
  void settingDescriptionMustNotClaimToCoverStart() {
    final String description = GlobalConfiguration.SERVER_SHUTDOWN_TIMEOUT.getDescription();

    assertThat(description)
        .as("the setting does not bound the shutdown-hook wait during start(), so its description must not say so")
        .doesNotContain("start()/stop()");
    assertThat(description).contains("this setting does not govern");
  }
}
