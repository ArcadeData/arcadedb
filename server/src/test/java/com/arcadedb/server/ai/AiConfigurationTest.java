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
package com.arcadedb.server.ai;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

class AiConfigurationTest {
  private static final String TEST_ROOT = "target/test-ai-config";

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(TEST_ROOT));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(TEST_ROOT));
  }

  @Test
  void notConfiguredWhenFileDoesNotExist() {
    final AiConfiguration config = new AiConfiguration(TEST_ROOT);
    config.load();

    assertThat(config.isConfigured()).isFalse();
    assertThat(config.getSubscriptionToken()).isEmpty();
    assertThat(config.getGatewayUrl()).isEqualTo("https://ai.arcadedb.com");
  }

  @Test
  void notConfiguredWhenTokenIsEmpty() throws Exception {
    writeConfigFile(new JSONObject().put("subscriptionToken", "").put("gatewayUrl", "https://ai.arcadedb.com"));

    final AiConfiguration config = new AiConfiguration(TEST_ROOT);
    config.load();

    assertThat(config.isConfigured()).isFalse();
  }

  @Test
  void configuredWhenTokenIsPresent() throws Exception {
    writeConfigFile(new JSONObject().put("subscriptionToken", "test-token-123").put("gatewayUrl", "https://custom-gateway.example.com"));

    final AiConfiguration config = new AiConfiguration(TEST_ROOT);
    config.load();

    assertThat(config.isConfigured()).isTrue();
    assertThat(config.getSubscriptionToken()).isEqualTo("test-token-123");
    assertThat(config.getGatewayUrl()).isEqualTo("https://custom-gateway.example.com");
  }

  @Test
  void toJsonDoesNotExposeToken() {
    final AiConfiguration config = new AiConfiguration(TEST_ROOT);
    config.load();

    final JSONObject json = config.toJSON();
    assertThat(json.has("subscriptionToken")).isFalse();
    assertThat(json.getBoolean("configured")).isFalse();
  }

  @Test
  void handlesCorruptConfigFile() throws Exception {
    final File configDir = Paths.get(TEST_ROOT, "config").toFile();
    configDir.mkdirs();
    try (final OutputStreamWriter writer = new OutputStreamWriter(
        new FileOutputStream(new File(configDir, "ai.json")), StandardCharsets.UTF_8)) {
      writer.write("not valid json{{{");
    }

    final AiConfiguration config = new AiConfiguration(TEST_ROOT);
    config.load(); // Should not throw

    assertThat(config.isConfigured()).isFalse();
  }

  private void writeConfigFile(final JSONObject json) throws Exception {
    final File configDir = Paths.get(TEST_ROOT, "config").toFile();
    configDir.mkdirs();
    try (final OutputStreamWriter writer = new OutputStreamWriter(
        new FileOutputStream(new File(configDir, "ai.json")), StandardCharsets.UTF_8)) {
      writer.write(json.toString(2));
    }
  }
}
