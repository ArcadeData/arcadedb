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
package com.arcadedb.bench.server;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ServerUnderTestRedactTest {
  @Test
  void passwordSettingsAreRedactedWhateverTheCase() {
    final Map<String, String> settings = new LinkedHashMap<>();
    settings.put("arcadedb.server.rootPassword", "secret1");
    settings.put("arcadedb.server.rootPasswordPath", "secret2");
    settings.put("some.PASSWORD.x", "secret3");
    settings.put("arcadedb.backup.enabled", "false");

    final JSONObject json = ServerUnderTest.redact(settings);

    assertThat(json.getString("arcadedb.server.rootPassword")).isEqualTo("***");
    assertThat(json.getString("arcadedb.server.rootPasswordPath")).isEqualTo("***");
    assertThat(json.getString("some.PASSWORD.x")).isEqualTo("***");
    assertThat(json.getString("arcadedb.backup.enabled")).isEqualTo("false");
    assertThat(json.toString()).doesNotContain("secret");
  }
}
