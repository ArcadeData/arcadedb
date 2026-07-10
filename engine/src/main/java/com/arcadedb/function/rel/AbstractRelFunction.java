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
package com.arcadedb.function.rel;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;

/**
 * Abstract base class for relationship functions.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractRelFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "rel." + getSimpleName();
  }

  protected abstract String getSimpleName();

  /**
   * Converts an input object to an Edge.
   *
   * @param input the input object
   * @return the Edge, or null if not an edge
   */
  protected Edge toEdge(final Object input) {
    if (input == null)
      return null;

    if (input instanceof Edge edge)
      return edge;

    if (input instanceof Document doc) {
      return doc.getRecord() instanceof Edge edge ? edge : null;
    }

    if (input instanceof Result result) {
      final Record record = result.getRecord().orElse(null);
      return record instanceof Edge edge ? edge : null;
    }

    if (input instanceof Identifiable identifiable) {
      return identifiable.getRecord() instanceof Edge edge ? edge : null;
    }

    return null;
  }
}
