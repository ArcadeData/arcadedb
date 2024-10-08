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
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.handlers.PathHandler;

import static com.arcadedb.server.ServerPlugin.INSTALLATION_PRIORITY.BEFORE_HTTP_ON;

public interface ServerPlugin {
  enum INSTALLATION_PRIORITY {BEFORE_HTTP_ON, AFTER_HTTP_ON, AFTER_DATABASES_OPEN}

  default void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
    // DEFAULT IMPLEMENTATION
  }

  void startService();

  default void stopService() {
    // DEFAULT IMPLEMENTATION
  }

  default void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    // DEFAULT IMPLEMENTATION
  }

  default INSTALLATION_PRIORITY getInstallationPriority() {
    return BEFORE_HTTP_ON;
  }
}
