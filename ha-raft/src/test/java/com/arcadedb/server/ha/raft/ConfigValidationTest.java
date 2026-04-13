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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates default values for HA-related {@link GlobalConfiguration} entries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ConfigValidationTest {

  @Test
  void haReplicationLagWarningHasDefault() {
    assertThat(GlobalConfiguration.HA_REPLICATION_LAG_WARNING.getValueAsLong()).isEqualTo(1000L);
  }

  @Test
  void haReplicationIncomingPortsDefaultStartsWith2424() {
    // The default Raft port range starts at 2424
    assertThat(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS.getValueAsString()).startsWith("2424");
  }
}
