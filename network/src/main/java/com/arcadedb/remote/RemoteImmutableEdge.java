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

import com.arcadedb.database.JSONSerializer;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;

public class RemoteImmutableEdge extends RemoteImmutableDocument implements Edge {
  private final RID out;
  private final RID in;

  public RemoteImmutableEdge(final RemoteDatabase database, final Map<String, Object> properties) {
    super(database, properties);
    this.out = new RID(remoteDatabase, (String) properties.get(RemoteProperty.OUT_PROPERTY));
    this.in = new RID(remoteDatabase, (String) properties.get(RemoteProperty.IN_PROPERTY));
  }

  @Override
  public RID getOut() {
    return out;
  }

  @Override
  public Vertex getOutVertex() {
    return loadVertex(out);
  }

  @Override
  public RID getIn() {
    return in;
  }

  @Override
  public Vertex getInVertex() {
    return loadVertex(in);
  }

  @Override
  public Vertex getVertex(final Vertex.Direction iDirection) {
    if (iDirection == Vertex.Direction.OUT)
      return getOutVertex();
    else
      return getInVertex();
  }

  @Override
  public MutableEdge modify() {
    return new RemoteMutableEdge(this);
  }

  @Override
  public void delete() {
    remoteDatabase.command("sql", "delete from " + rid);
  }

  private Vertex loadVertex(final RID rid) {
    final ResultSet result = remoteDatabase.query("sql", "select from " + rid);
    if (result.hasNext())
      return result.next().getVertex().get();

    return null;
  }

  @Override
  public Edge asEdge() {
    return this;
  }

  @Override
  public Edge asEdge(final boolean loadContent) {
    if (loadContent)
      checkForLazyLoading();
    return this;
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> result = new HashMap<>(map);
    if (includeMetadata) {
      result.put(Property.CAT_PROPERTY, "e");
      result.put(Property.TYPE_PROPERTY, getTypeName());
      if (getIdentity() != null)
        result.put(RID_PROPERTY, getIdentity().toString());
    }
    return result;
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject result = new JSONSerializer(database).map2json(map, type, includeMetadata);
    if (includeMetadata) {
      result.put(Property.CAT_PROPERTY, "e");
      result.put(Property.TYPE_PROPERTY, getTypeName());
      if (getIdentity() != null)
        result.put(RID_PROPERTY, getIdentity().toString());
    }
    return result;
  }

}
