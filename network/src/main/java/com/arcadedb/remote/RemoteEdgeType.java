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
package com.arcadedb.remote;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.EdgeType;

/**
 * Edge type used by {@link RemoteDatabase} class. The metadata are cached from the server until the schema is changed or
 * {@link RemoteSchema#reload()} is called.
 * <p>
 * This class is not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class RemoteEdgeType extends RemoteDocumentType implements EdgeType {
  private final boolean bidirectional;
  private final boolean lightweight;
  private final boolean unique;

  RemoteEdgeType(final RemoteDatabase remoteDatabase, final Result record, final boolean bidirectional,
                 final boolean lightweight, final boolean unique) {
    super(remoteDatabase, record);
    this.bidirectional = bidirectional;
    this.lightweight = lightweight;
    this.unique = unique;
  }

  /**
   * Refused as {@link EdgeType#newRecord()} does (issue #12190): an edge is created from its two vertices, and the inherited
   * document record saved an edge without them.
   */
  @Override
  public MutableDocument newRecord() {
    throw new UnsupportedOperationException(
        "newRecord() is not supported on an edge type. Create the edge from its source vertex with newEdge()");
  }

  @Override
  public boolean isBidirectional() {
    return bidirectional;
  }

  @Override
  public boolean isLightweight() {
    return lightweight;
  }

  @Override
  public boolean isUnique() {
    return unique;
  }
}
