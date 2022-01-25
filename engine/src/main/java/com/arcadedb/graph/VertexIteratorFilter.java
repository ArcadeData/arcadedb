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
package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;

import java.util.NoSuchElementException;
import java.util.logging.Level;

public class VertexIteratorFilter extends IteratorFilterBase<Vertex> {
  public VertexIteratorFilter(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes) {
    super(database, current, edgeTypes);
  }

  @Override
  public boolean hasNext() {
    return super.hasNext(false);
  }

  @Override
  public Vertex next() {
    hasNext();

    if (next == null)
      throw new NoSuchElementException();

    try {
      return next.asVertex();
    } catch (SchemaException e) {
      LogManager.instance().log(this, Level.WARNING, "Error on loading vertex %s from edge %s", e, next, nextEdge);
      throw e;
    } finally {
      next = null;
    }
  }
}
