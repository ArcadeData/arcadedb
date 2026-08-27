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

import java.util.Arrays;

/**
 * One node's neighbours and the edge-property value of each edge reaching them, produced together.
 * <p>
 * {@code weights[j]} belongs to the edge to {@code neighbors[j]} <em>by construction</em>: both arrays are filled
 * from one walk of the same edges, by {@link GraphTraversalProvider#edgeWeightsForSlice} for a single (type,
 * direction) slice and by {@link GraphTraversalProvider#edgeWeightsOf} for the concatenation of them. Handing the
 * two back together is what removes the reconciliation step - and every place that reconciled them afterwards got
 * it wrong, which is issues #6301 and #6315.
 *
 * @param neighbors dense neighbour ids
 * @param weights   the property value of the edge to the neighbour at the same index
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record NodeEdgeWeights(int[] neighbors, double[] weights) {
  /**
   * By content, not by array identity. A record's generated {@code equals} compares its components with
   * {@code Object.equals}, which for two arrays is identity - so two separately built rows describing the very
   * same neighbourhood would answer "not equal", and one used as a map key would never be found again. Nobody
   * relies on that today; the point of writing it out is that the reader who eventually does will not have to
   * discover it first.
   */
  @Override
  public boolean equals(final Object other) {
    if (this == other)
      return true;
    return other instanceof NodeEdgeWeights that
        && Arrays.equals(neighbors, that.neighbors) && Arrays.equals(weights, that.weights);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(neighbors) + Arrays.hashCode(weights);
  }

  @Override
  public String toString() {
    return "NodeEdgeWeights[neighbors=" + Arrays.toString(neighbors) + ", weights=" + Arrays.toString(weights) + "]";
  }
}
