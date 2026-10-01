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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8840: validation of {@code arcadedb.postgres.ssl}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresSslHelperTest {

  private static ContextConfiguration modeOf(final String mode) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.POSTGRES_SSL, mode);
    return configuration;
  }

  @Test
  void disabledNeedsNoKeyStore() {
    assertThat(new PostgresSslHelper(modeOf("disabled")).getTlsMode()).isEqualTo(PostgresSslHelper.TlsMode.DISABLED);
    assertThat(PostgresSslHelper.disabled().getTlsMode()).isEqualTo(PostgresSslHelper.TlsMode.DISABLED);
  }

  @Test
  void unknownModeIsRefusedNamingTheSetting() {
    assertThatThrownBy(() -> new PostgresSslHelper(modeOf("maybe"))).isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("arcadedb.postgres.ssl").hasMessageContaining("maybe");
  }

  @Test
  void enabledWithoutKeyStoreIsRefusedAtStartup() {
    assertThatThrownBy(() -> new PostgresSslHelper(modeOf("OPTIONAL"))).isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("key store path");
    assertThatThrownBy(() -> new PostgresSslHelper(modeOf("REQUIRED"))).isInstanceOf(ConfigurationException.class);
  }
}
