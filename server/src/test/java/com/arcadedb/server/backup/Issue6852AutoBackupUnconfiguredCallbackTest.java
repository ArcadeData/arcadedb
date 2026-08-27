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
package com.arcadedb.server.backup;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Issue #6852. The server no longer notifies a plugin before it has been configured, but the plugin must survive such a
 * callback on its own too: it is the one that logged ten SEVERE stack traces per startup, and a lifecycle callback is
 * announced from whichever thread mutated the registry, so "nobody would call it that early" is not something this class
 * can assume. With no scheduler published there is simply nothing to reconcile - {@code startService()} schedules
 * everything that exists by then - so the callback has to be a no-op rather than a dereference of a null server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6852AutoBackupUnconfiguredCallbackTest {
  @Test
  void lifecycleCallbacksOnAnUnconfiguredPluginAreANoOp() {
    final AutoBackupSchedulerPlugin plugin = new AutoBackupSchedulerPlugin();

    assertThatCode(() -> plugin.onDatabaseRegistered("neverConfigured")).doesNotThrowAnyException();
    assertThatCode(() -> plugin.onDatabaseUnregistered("neverConfigured")).doesNotThrowAnyException();
    assertThatCode(() -> plugin.scheduleDatabase("neverConfigured")).doesNotThrowAnyException();
    assertThatCode(() -> plugin.cancelDatabase("neverConfigured")).doesNotThrowAnyException();
  }
}
