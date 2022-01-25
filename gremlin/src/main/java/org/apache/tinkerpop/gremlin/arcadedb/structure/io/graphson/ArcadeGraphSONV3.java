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
package org.apache.tinkerpop.gremlin.arcadedb.structure.io.graphson;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.util.*;

import static org.apache.tinkerpop.gremlin.arcadedb.structure.io.ArcadeIoRegistry.newRID;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public class ArcadeGraphSONV3 extends ArcadeGraphSON {

  protected static final Map<Class, String> TYPES = Collections.unmodifiableMap(new LinkedHashMap<>() {
      {
          put(RID.class, "RID");
      }
  });

  private final Database database;

  public ArcadeGraphSONV3(final Database database) {
    super("arcade-graphson-v3");
    this.database = database;

    addDeserializer(Edge.class, new EdgeJacksonDeserializer());
    addDeserializer(Vertex.class, new VertexJacksonDeserializer());
  }

  @Override
  public Map<Class, String> getTypeDefinitions() {
    return TYPES;
  }

  /**
   * Created by Enrico Risa on 06/09/2017.
   */
  public class EdgeJacksonDeserializer extends AbstractObjectDeserializer<Edge> {

    public EdgeJacksonDeserializer() {
      super(Edge.class);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Edge createObject(final Map<String, Object> edgeData) {
      return new DetachedEdge(newRID(database, edgeData.get(GraphSONTokens.ID)), edgeData.get(GraphSONTokens.LABEL).toString(),
          (Map) edgeData.get(GraphSONTokens.PROPERTIES), newRID(database, edgeData.get(GraphSONTokens.OUT)), edgeData.get(GraphSONTokens.OUT_LABEL).toString(),
          newRID(database, edgeData.get(GraphSONTokens.IN)), edgeData.get(GraphSONTokens.IN_LABEL).toString());
    }
  }

  /**
   * Created by Enrico Risa on 06/09/2017.
   */
  public class VertexJacksonDeserializer extends AbstractObjectDeserializer<Vertex> {

    public VertexJacksonDeserializer() {
      super(Vertex.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex createObject(final Map<String, Object> vertexData) {
      return new DetachedVertex(newRID(database, vertexData.get(GraphSONTokens.ID)), vertexData.get(GraphSONTokens.LABEL).toString(),
          (Map<String, Object>) vertexData.get(GraphSONTokens.PROPERTIES));
    }
  }
}
