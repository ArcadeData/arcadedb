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

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

public class RemoteImmutableEdge extends RemoteImmutableDocument implements Edge {
  private final RID out;
  private final RID in;

  public RemoteImmutableEdge(final RemoteDatabase database, final Map<String, Object> properties) {
    super(database, properties);
    this.out = new RID(remoteDatabase, (String) properties.get("@out"));
    this.in = new RID(remoteDatabase, (String) properties.get("@in"));

    map.remove("@cat");
    map.remove("@type");
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
  public Vertex getVertex(final Vertex.DIRECTION iDirection) {
    if (iDirection == Vertex.DIRECTION.OUT)
      return getOutVertex();
    else
      return getInVertex();
  }

  @Override
  public synchronized MutableEdge modify() {
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
}
