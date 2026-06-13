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

import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the server exposes a shared {@link ObservationRegistry} that the optional tracing plugin
 * attaches to. By default the registry carries no handlers, so Observations are no-ops.
 */
class ObservationRegistryIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void serverExposesObservationRegistry() {
    final ObservationRegistry registry = getServer(0).getObservationRegistry();
    assertThat(registry).isNotNull();
    // With no tracing plugin attached, the registry has no handlers, so it reports itself as a
    // no-op: Observations are zero-overhead until the tracing plugin registers a handler. This
    // guarantees the default (tracing-disabled) configuration behaves identically to before.
    assertThat(registry.isNoop()).isTrue();
  }
}
