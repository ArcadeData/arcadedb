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
package com.arcadedb.function.create;

import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.graph.VirtualVertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * create.vNode(labels, properties = {}) - Create a virtual node: a node that is not persisted, with the given labels and
 * properties, as APOC's {@code apoc.create.vNode}. It is a real node value ({@code labels()}, {@code properties()},
 * {@code n.prop}, {@code id()} work on it) with a negative id, not a map.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CreateVNode extends AbstractCreateFunction {
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
  public Object execute(final Object[] args, final CommandContext context) {
    return create(getName(), args[0], args.length > 1 ? args[1] : null, context);
  }

  /**
   * Builds the virtual node; shared with the {@code CALL create.vNode(...) YIELD node} form.
   */
  @SuppressWarnings("unchecked")
  public static VirtualVertex create(final String name, final Object labels, final Object properties,
      final CommandContext context) {
    if (properties != null && !(properties instanceof Map))
      throw new TypeMismatchException(name + "() expects the properties as a map, got " + properties.getClass().getSimpleName());
    return new VirtualVertex(context != null ? context.getDatabase() : null, extractLabels(labels),
        (Map<String, Object>) properties);
  }

  private static List<String> extractLabels(final Object input) {
    final List<String> labels = new ArrayList<>();
    if (input instanceof String label)
      labels.add(label);
    else if (input instanceof Collection<?> collection)
      for (final Object item : collection)
        if (item != null)
          labels.add(item.toString());
    return labels;
  }
}
