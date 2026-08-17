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

/**
 * One node's neighbours and the edge-property value of each edge reaching them, produced together.
 * <p>
 * {@code weights[j]} belongs to the edge to {@code neighbors[j]} <em>by construction</em>: both arrays are filled
 * from one walk of the same per-type, per-direction adjacency slices, each of which is positional against
 * {@link GraphTraversalProvider#getEdgeProperty}. Handing the two back together is what removes the reconciliation
 * step - and every place that reconciled them afterwards got it wrong, which is issue #6301.
 *
 * @param neighbors dense neighbour ids
 * @param weights   the property value of the edge to the neighbour at the same index
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record NodeEdgeWeights(int[] neighbors, double[] weights) {
}
