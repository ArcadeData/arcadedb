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
package com.arcadedb.query.opencypher.functions.create;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * create.vNode(labels, properties) - Create a virtual node (map representation).
 * <p>
 * Virtual nodes are not persisted to the database. They are useful for
 * temporary data structures or for returning computed results.
 * </p>
 *
 * @author ArcadeDB Team
 */
public class CreateVNode extends AbstractCreateFunction {
  private static long virtualIdCounter = 0;

  @Override
  protected String getSimpleName() {
    return "vNode";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Create a virtual node (not persisted) with the given labels and properties";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    final List<String> labels = extractLabels(args[0]);
    final Map<String, Object> properties = args.length > 1 && args[1] != null
        ? new HashMap<>((Map<String, Object>) args[1])
        : new HashMap<>();

    // Create a virtual node representation
    final Map<String, Object> vNode = new HashMap<>();
    vNode.put("_type", "vNode");
    vNode.put("_id", "vNode:" + (++virtualIdCounter));
    vNode.put("_labels", labels);
    vNode.putAll(properties);

    return vNode;
  }

  @SuppressWarnings("unchecked")
  private List<String> extractLabels(final Object input) {
    final List<String> labels = new ArrayList<>();
    if (input == null)
      return labels;

    if (input instanceof String) {
      labels.add((String) input);
    } else if (input instanceof Collection) {
      for (final Object item : (Collection<?>) input) {
        if (item != null)
          labels.add(item.toString());
      }
    }

    return labels;
  }
}
