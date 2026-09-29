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
package com.arcadedb.server.gremlin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.StaticBaseServerTest;

/**
 * Gives a test's Gremlin Server a port of its own instead of TinkerPop's default 8182, which two parallel builds on one
 * machine would fight over (issue #8578).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GremlinTestPorts {
  private GremlinTestPorts() {
  }

  /**
   * Draws a free port and sets it as the {@code gremlin.port} server setting, which {@code GremlinServerPlugin} copies onto
   * the Gremlin Server settings.
   *
   * @return the port the Gremlin Server of this configuration will listen on
   */
  static int assign(final ContextConfiguration config) {
    final int port = StaticBaseServerTest.allocateFreePorts(1)[0];
    // AS TEXT, THE WAY A SYSTEM PROPERTY OR THE COMMAND LINE DELIVERS IT: THE PLUGIN COERCES IT TO THE int SETTING
    config.setValue("gremlin.port", String.valueOf(port));
    return port;
  }
}
