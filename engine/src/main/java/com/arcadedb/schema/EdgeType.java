package com.arcadedb.schema;/*
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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.Edge;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Schema Edge Type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface EdgeType extends DocumentType {
  @Override
  default MutableDocument newRecord() {
    throw new UnsupportedOperationException();
  }

  @Override
  default byte getType() {
    return Edge.RECORD_TYPE;
  }

  boolean isBidirectional();

  /**
   * True when every edge of this type is stored lightweight: as a pair of pointers inside the two vertices' edge
   * lists, with no edge record and therefore no properties. Orthogonal to {@link #isBidirectional()}.
   * <p>
   * A lightweight edge is identified by the triple (type, out vertex, in vertex): with no properties, two of them
   * over the same ordered pair are indistinguishable and are therefore the same edge. Declare {@link #isUnique()} to
   * have that enforced.
   * <p>
   * The flag governs <b>writes only</b>. Reads never consult it: an edge-list entry whose edge position is negative
   * is a lightweight edge whatever the type declares. That is what keeps databases written before the flag existed,
   * and types holding both shapes, working unchanged.
   */
  boolean isLightweight();

  /**
   * True when at most one edge of this type may connect a given ordered pair of vertices.
   * <p>
   * How it is enforced depends on the storage: a regular edge type gets a unique index on {@code (@out, @in)}, so the
   * check is a O(log n) index probe; a lightweight edge type has no records to index, so the check is a scan of the
   * source vertex's edge list, O(degree). Uniqueness is therefore <b>cheaper on regular edges</b> than on lightweight
   * ones, which is worth knowing before declaring it on a high-degree type.
   */
  boolean isUnique();
}
