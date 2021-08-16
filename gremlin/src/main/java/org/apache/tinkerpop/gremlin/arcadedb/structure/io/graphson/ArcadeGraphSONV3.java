/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.arcadedb.structure.io.ArcadeIoRegistry.isRID;
import static org.apache.tinkerpop.gremlin.arcadedb.structure.io.ArcadeIoRegistry.newRID;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public class ArcadeGraphSONV3 extends ArcadeGraphSON {

  protected static final Map<Class, String> TYPES = Collections.unmodifiableMap(new LinkedHashMap<Class, String>() {
    {
      put(RID.class, "RID");
    }
  });

  private final Database database;

  public ArcadeGraphSONV3(final Database database) {
    super("arcade-graphson-v3");
    this.database = database;

    addSerializer(RID.class, new RIDJacksonSerializer());

    addDeserializer(RID.class, new RIDJacksonDeserializer());
    addDeserializer(Edge.class, new EdgeJacksonDeserializer());
    addDeserializer(Vertex.class, new VertexJacksonDeserializer());
    addDeserializer(Map.class, (JsonDeserializer) new RIDDeserializer());
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

  final class RIDDeserializer extends AbstractObjectDeserializer<Object> {

    public RIDDeserializer() {
      super(Object.class);
    }

    @Override
    public Object createObject(Map<String, Object> data) {

      if (isRID(data)) {
        return newRID(database, data);
      }
      return data;
    }

  }

  public class RIDJacksonDeserializer extends StdDeserializer<RID> {
    protected RIDJacksonDeserializer() {
      super(RID.class);
    }

    @Override
    public RID deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
      final String rid = deserializationContext.readValue(jsonParser, String.class);
      return new RID(database, rid);
    }
  }

}
