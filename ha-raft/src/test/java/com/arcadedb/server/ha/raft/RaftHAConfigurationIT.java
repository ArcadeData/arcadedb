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

import com.arcadedb.server.ServerException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RaftHAConfigurationIT {

  @Test
  void invalidPeerAddressRejected() {
    // Mix non-localhost IPs with localhost — this should be rejected
    assertThatThrownBy(() -> RaftHAServer.parsePeerList("192.168.0.1:2434,192.168.0.1:2435,localhost:2434", 2434))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Found a localhost");
  }
}
