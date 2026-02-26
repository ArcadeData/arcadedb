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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;

/**
 * Loads and provides access to the AI assistant configuration from config/ai.json.
 */
public class AiConfiguration {
  private static final String DEFAULT_GATEWAY_URL = "https://ai.arcadedb.com";

  private final String rootPath;

  private volatile String subscriptionToken = "";
  private volatile String gatewayUrl        = DEFAULT_GATEWAY_URL;

  public AiConfiguration(final String rootPath) {
    this.rootPath = rootPath;
  }

  public synchronized void load() {
    final File configFile = getConfigFile();
    if (!configFile.exists())
      return;

    try {
      final String content = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);
      final JSONObject json = new JSONObject(content);

      subscriptionToken = json.getString("subscriptionToken", "");
      gatewayUrl = json.getString("gatewayUrl", DEFAULT_GATEWAY_URL);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error loading AI configuration: %s", e.getMessage());
    }
  }

  public boolean isConfigured() {
    return subscriptionToken != null && !subscriptionToken.isEmpty();
  }

  public String getSubscriptionToken() {
    return subscriptionToken;
  }

  public String getGatewayUrl() {
    return gatewayUrl;
  }

  public synchronized JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("configured", isConfigured());
    json.put("gatewayUrl", gatewayUrl);
    return json;
  }

  private File getConfigFile() {
    return Paths.get(rootPath, "config", "ai.json").toFile();
  }
}
