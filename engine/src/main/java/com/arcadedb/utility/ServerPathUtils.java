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
package com.arcadedb.utility;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import java.io.*;

/**
 * Utility class for server path configuration.
 *
 * Originally from IntegrationUtils in integration module.
 * Moved to engine module to avoid server → integration dependency.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ServerPathUtils {
  private ServerPathUtils() {
    // Utility class
  }

  /**
   * Sets the root path for the server configuration.
   * If not configured, it will auto-detect based on the existence of config directory.
   *
   * @param configuration The server configuration context
   * @return The server root path (usually "." or "..")
   */
  public static String setRootPath(final ContextConfiguration configuration) {
    String serverRootPath = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PATH);
    if (serverRootPath == null) {
      serverRootPath = new File("config").exists() ? "." : new File("../config").exists() ? ".." : ".";
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, serverRootPath);
    }
    return serverRootPath;
  }
}
