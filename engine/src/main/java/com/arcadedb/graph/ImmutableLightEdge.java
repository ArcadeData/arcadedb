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

import com.arcadedb.database.Database;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class ImmutableLightEdge extends ImmutableDocument implements LightEdge {
  private final RID out;
  private final RID in;

  public ImmutableLightEdge(final Database graph, final DocumentType type, final RID edgeRID, final RID out, final RID in) {
    super(graph, type, edgeRID, null);
    this.out = out;
    this.in = in;
  }

  @Override
  public synchronized Object get(final String propertyName) {
    return null;
  }

  public synchronized MutableEdge modify() {
    throw new IllegalStateException("Lightweight edges cannot be modified");
  }

  @Override
  public RID getOut() {
    checkForLazyLoading();
    return out;
  }

  @Override
  public Vertex getOutVertex() {
    checkForLazyLoading();
    return out.asVertex();
  }

  @Override
  public RID getIn() {
    checkForLazyLoading();
    return in;
  }

  @Override
  public Vertex getInVertex() {
    checkForLazyLoading();
    return in.asVertex();
  }

  @Override
  public Vertex getVertex(final Vertex.DIRECTION iDirection) {
    checkForLazyLoading();
    if (iDirection == Vertex.DIRECTION.OUT)
      return (Vertex) out.getRecord();
    else
      return (Vertex) in.getRecord();
  }

  @Override
  public synchronized Set<String> getPropertyNames() {
    return Collections.EMPTY_SET;
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }

  @Override
  protected boolean checkForLazyLoading() {
    return false;
  }

  @Override
  public synchronized Map<String, Object> toMap() {
    return Collections.emptyMap();
  }

  @Override
  public Edge asEdge() {
    return this;
  }

  @Override
  public Edge asEdge(final boolean loadContent) {
    return this;
  }

  @Override
  public synchronized JSONObject toJSON() {
    return new JSONObject();
  }

  @Override
  public synchronized String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(out.toString());
    buffer.append("<->");
    buffer.append(in.toString());
    return buffer.toString();
  }
}
