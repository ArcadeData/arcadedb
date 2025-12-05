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
package com.arcadedb.integration.misc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.utility.ServerPathUtils;

/**
 * Deprecated utility class for common packages.
 * Use ServerPathUtils from engine module instead.
 *
 * This class is kept for backward compatibility only.
 * New code should use ServerPathUtils directly.
 *
 * @deprecated Use {@link ServerPathUtils#setRootPath(ContextConfiguration)} instead
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Deprecated(forRemoval = true, since = "25.11.1")
public class IntegrationUtils {
  private IntegrationUtils() {
    // Utility class
  }

  /**
   * Sets the root path for the server configuration.
   *
   * @deprecated Use {@link ServerPathUtils#setRootPath(ContextConfiguration)} instead
   * @param configuration The server configuration context
   * @return The server root path (usually "." or "..")
   */
  @Deprecated(forRemoval = true, since = "25.11.1")
  public static String setRootPath(final ContextConfiguration configuration) {
    return ServerPathUtils.setRootPath(configuration);
  }
}
