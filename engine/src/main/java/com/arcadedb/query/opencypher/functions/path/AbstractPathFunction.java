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
package com.arcadedb.query.opencypher.functions.path;

import com.arcadedb.function.StatelessFunction;

import java.util.List;
import java.util.Map;

/**
 * Abstract base class for path functions.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractPathFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "path." + getSimpleName();
  }

  protected abstract String getSimpleName();

  /**
   * Checks if an object is a path representation (list of alternating nodes and relationships).
   */
  protected boolean isPath(final Object obj) {
    if (obj instanceof List) {
      return true; // Assume list is a path
    }
    if (obj instanceof Map) {
      final Map<?, ?> map = (Map<?, ?>) obj;
      return map.containsKey("_type") && "path".equals(map.get("_type"));
    }
    return false;
  }
}
