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
package com.arcadedb.query.select;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.vector.LSMVectorIndex;

import java.util.List;

/**
 * Fluent builder for vector k-NN search. Provides options for approximate search and optional WHERE post-filtering.
 * <p>
 * #6873: a {@link Select#timeout} set on the select is honoured here too, between bucket indexes, before the merge
 * sort of what they returned, and per assembled result. Two limits are worth knowing about. The search inside a single
 * index is not interruptible, so the budget cannot cut one of those short. And a non-throwing budget that runs out
 * before result assembly begins answers with an <b>empty</b> list, not a truncated one - unlike
 * {@link SelectIterator}, which hands back the records it had already yielded, this path has almost all of its work
 * after the last checkpoint, and promoting an unassembled neighbour would mean loading a record past a deadline that
 * has already expired.
 */
public class SelectVectorBuilder {
  private final Select select;

  public SelectVectorBuilder(final Select select) {
    this.select = select;
  }

  public SelectVectorBuilder approximate(final boolean approximate) {
    select.vectorApproximate = approximate;
    return this;
  }

  /**
   * Re-ranks the PQ search of a PRODUCT index on the stored vectors, with
   * {@link LSMVectorIndex#DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE} candidates per requested neighbour, and returns exact
   * distances (issue #10514). It applies with {@link #approximate(boolean)} set and without it, where it overrides the index's
   * {@code rerankOversample}; the cost is one record read per candidate.
   */
  public SelectVectorBuilder rerank() {
    return rerank(LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE);
  }

  /**
   * @param oversample candidates per requested neighbour to re-rank exactly on the stored vectors; {@code 0} returns the PQ
   *                   scores as they are. When never called, {@code approximate(true)} returns the PQ scores and the exact
   *                   search takes the index's {@code rerankOversample}
   *
   * @see #rerank()
   */
  public SelectVectorBuilder rerank(final int oversample) {
    if (oversample < 0)
      throw new IllegalArgumentException("oversample must be >= 0, got " + oversample);
    select.vectorRerankOversample = oversample;
    return this;
  }

  public SelectWhereLeftBlock where() {
    select.state = Select.STATE.WHERE;
    return new SelectWhereLeftBlock(select);
  }

  /**
   * Runs the k-NN search and returns the neighbours as vertices, closest first. See the class Javadoc for what a
   * {@link Select#timeout} does to this call.
   */
  public List<SelectVectorResult<Vertex>> vertices() {
    return executeVector();
  }

  /**
   * Runs the k-NN search and returns the neighbours as documents, closest first. See the class Javadoc for what a
   * {@link Select#timeout} does to this call.
   */
  public List<SelectVectorResult<Document>> documents() {
    return executeVector();
  }

  @SuppressWarnings("unchecked")
  private <T extends Document> List<SelectVectorResult<T>> executeVector() {
    select.compile();
    return new SelectExecutor(select).executeVector();
  }
}
