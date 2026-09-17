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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7296 at the writer the report names.
 * <p>
 * {@code SET SERVER SETTING arcadedb.server.mode prodction} returned 200 and stored {@code "prodction"}, so
 * production mode never engaged: {@code AbstractServerHttpHandler} decides error-detail concealment by comparing
 * the stored value with {@code "production"}, found it different, and served the deployment the DEVELOPMENT
 * behaviour. The allow-list that exists to refuse exactly that value was enforced on the configuration-file
 * writer and on none of the administrative ones.
 * <p>
 * Asserted through {@link ServerControlPlane#applySetting} rather than over HTTP because that IS the shared
 * writer: since the control-plane refactor the HTTP {@code set server setting} command and the gRPC
 * {@code SetServerSetting} RPC both land here, so one test covers both transports. The
 * {@code IllegalArgumentException} it now throws is what the HTTP layer already maps to 400 and gRPC to
 * {@code INVALID_ARGUMENT}, the same way a bad Boolean has been handled since #7124.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7296SetServerSettingAllowListTest {

  @Test
  void anUnlistedServerModeIsRefusedInsteadOfStoredVerbatim() {
    final ContextConfiguration configuration = new ContextConfiguration();

    assertThatThrownBy(() -> ServerControlPlane.applySetting(configuration, GlobalConfiguration.SERVER_MODE.getKey(),
        "prodction")).isInstanceOf(IllegalArgumentException.class);

    assertThat(configuration.hasValue(GlobalConfiguration.SERVER_MODE.getKey()))
        .as("a refused value must not reach the overlay at all")
        .isFalse();
  }

  @Test
  void aListedServerModeIsStillStoredAndNormalised() {
    final ContextConfiguration configuration = new ContextConfiguration();

    ServerControlPlane.applySetting(configuration, GlobalConfiguration.SERVER_MODE.getKey(), "PRODUCTION");

    assertThat(configuration.getValueAsString(GlobalConfiguration.SERVER_MODE))
        .as("every reader compares the stored value with \"production\"")
        .isEqualTo("production");
  }

  /** The other allow-listed server settings the same writer reaches. */
  @Test
  void theOtherAllowListedServerSettingsAreRefusedToo() {
    final ContextConfiguration configuration = new ContextConfiguration();

    assertThatThrownBy(() -> ServerControlPlane.applySetting(configuration, GlobalConfiguration.HA_QUORUM.getKey(),
        "majorty")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ServerControlPlane.applySetting(configuration,
        GlobalConfiguration.HA_SERVER_ROLE.getKey(), "witness")).isInstanceOf(IllegalArgumentException.class);
  }

  /** A key that names no declared setting is still stored verbatim, as this endpoint has long allowed. */
  @Test
  void anUndeclaredKeyIsStillStoredVerbatim() {
    final ContextConfiguration configuration = new ContextConfiguration();

    ServerControlPlane.applySetting(configuration, "arcadedb.custom.notDeclared", "anything");

    assertThat(configuration.getValueAsString("arcadedb.custom.notDeclared", null)).isEqualTo("anything");
  }
}
