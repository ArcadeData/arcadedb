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

import com.arcadedb.database.RID;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Not intended to be used by the end-user. Internal only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see Vertex
 */
@ExcludeFromJacocoGeneratedReport
public interface VertexInternal extends Vertex {
  RID getOutEdgesHeadChunk();

  RID getInEdgesHeadChunk();

  /**
   * The head of the outgoing edge list, read by a writer of the list (an append, a head flip, the serializer, the commit's own
   * merges) rather than by a traversal: a {@link MutableVertex} records a traversal read of a head so the commit can tell a
   * decision taken on a head a concurrent transaction moved (issue #12320), and this read is not one.
   */
  default RID peekOutEdgesHeadChunk() {
    return getOutEdgesHeadChunk();
  }

  /** {@link #peekOutEdgesHeadChunk()} for the incoming edge list. */
  default RID peekInEdgesHeadChunk() {
    return getInEdgesHeadChunk();
  }

  void setOutEdgesHeadChunk(RID newChunk);

  void setInEdgesHeadChunk(RID newChunk);
}
