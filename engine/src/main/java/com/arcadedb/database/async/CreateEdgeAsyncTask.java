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
package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.VertexInternal;

/**
 * Asynchronous Task that creates the edge that connects 2 vertices.
 */
public class CreateEdgeAsyncTask implements DatabaseAsyncTask {
    protected final Identifiable sourceVertex;
    protected final Identifiable destinationVertex;

    protected final String edgeType;
    protected final Object[] edgeAttributes;

    protected final boolean bidirectional;
    protected final boolean light;
    protected final NewEdgeCallback callback;

    public CreateEdgeAsyncTask(final Identifiable sourceVertex,
                               final Identifiable destinationVertex,
                               final String edgeType,
                               final Object[] edgeAttributes,
                               final boolean bidirectional,
                               final boolean light,
                               final NewEdgeCallback callback) {
        this.sourceVertex = sourceVertex;
        this.destinationVertex = destinationVertex;

        this.edgeType = edgeType;
        this.edgeAttributes = edgeAttributes;

        this.bidirectional = bidirectional;
        this.light = light;
        this.callback = callback;
    }

    @Override
    public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
        createEdge(database, sourceVertex, destinationVertex, false, false);
    }

    protected void createEdge(final DatabaseInternal database,
                              final Identifiable sourceVertex,
                              final Identifiable destinationVertex,
                              final boolean createdSourceVertex,
                              final boolean createdDestinationVertex) {

        final Edge edge;

        if (light)
            edge = database.getGraphEngine().newLightEdge((VertexInternal) sourceVertex.getRecord(), edgeType, destinationVertex.getIdentity(), bidirectional);
        else
            edge = database.getGraphEngine()
                    .newEdge((VertexInternal) sourceVertex.getRecord(), edgeType, destinationVertex.getIdentity(), bidirectional, edgeAttributes);

        if (callback != null)
            callback.call(edge, createdSourceVertex, createdDestinationVertex);
    }

    @Override
    public String toString() {
        return "CreateEdgeAsyncTask(" + sourceVertex + "->" + destinationVertex + ")";
    }
}
