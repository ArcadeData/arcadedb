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
import com.arcadedb.database.RID;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A lightweight edge returned from {@link Vertex#newEdge} on a type declared
 * {@link EdgeType#isLightweight() LIGHTWEIGHT}.
 * <p>
 * It is a {@link MutableEdge} <b>in type only</b>, so that {@code newEdge()} can keep its declared return type on a
 * lightweight edge type without a source-breaking signature change. Nothing about it is actually mutable: there is no
 * record behind it, so every mutator throws. {@code save()} is the deliberate exception - it is a no-op returning
 * {@code this}, because the edge is already connected by the time {@code newEdge()} returns and
 * {@code v.newEdge(...).save()} is a harmless idiom that should not fail.
 * <p>
 * <b>Only ever construct this on the write path.</b> Traversal materialises one edge object per edge-list entry and
 * must keep using {@link ImmutableLightEdge}, which carries no {@link com.arcadedb.database.MutableDocument}
 * machinery. Unifying the two would silently make every traversal heavier.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MutableLightEdge extends MutableEdge implements LightEdge {

  public MutableLightEdge(final Database graph, final EdgeType type, final RID out, final RID in) {
    super(graph, type, new LightEdgeRID(graph, type.getFirstBucketId(), out, in), out, in, true);
  }

  @Override
  public MutableEdge modify() {
    return this;
  }

  @Override
  public boolean isDirty() {
    return false;
  }

  @Override
  public Object get(final String propertyName) {
    if ("@in".equals(propertyName))
      return in;
    if ("@out".equals(propertyName))
      return out;
    return null;
  }

  @Override
  public boolean has(final String propertyName) {
    return "@in".equals(propertyName) || "@out".equals(propertyName);
  }

  @Override
  public Set<String> getPropertyNames() {
    return Collections.emptySet();
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    return includeMetadata ? lightEdgeMetadata() : new LinkedHashMap<>();
  }

  /**
   * Same answer as {@link ImmutableLightEdge#toJSON(boolean)}, so the form of an edge does not depend on whether the caller
   * created it or read it (issue #9839): metadata, {@code @rid} included, exactly when it is asked for. With
   * {@code includeMetadata=false} that is {@code {}}, as for any record-backed edge without properties: the
   * {@link com.arcadedb.database.Document#toJSON(boolean)} contract names the endpoints as metadata, and a caller that wants
   * to tell edges apart asks for it.
   */
  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    return includeMetadata ? new JSONObject(lightEdgeMetadata()) : new JSONObject();
  }

  /**
   * Refused with the {@link IllegalArgumentException} {@link GraphEngine#newEdge} raises for the same mistake (issue #12871). An
   * empty property map describes no property and is accepted, as it is on creation.
   */
  @Override
  public MutableEdge set(final Object... properties) {
    if (GraphEngine.describesProperties(properties))
      throw cannotHaveProperties();
    return this;
  }

  @Override
  public MutableEdge set(final Map<String, Object> properties) {
    if (!properties.isEmpty())
      throw cannotHaveProperties();
    return this;
  }

  @Override
  public MutableEdge set(final String name, final Object value) {
    throw cannotHaveProperties();
  }

  @Override
  public MutableEdge set(final String name1, final Object value1, final String name2, final Object value2) {
    throw cannotHaveProperties();
  }

  @Override
  public MutableEdge set(final String name1, final Object value1, final String name2, final Object value2,
                         final String name3, final Object value3) {
    throw cannotHaveProperties();
  }

  @Override
  public MutableEdge fromMap(final Map<String, Object> map) {
    if (!map.isEmpty())
      throw cannotHaveProperties();
    return this;
  }

  /**
   * An empty object is accepted, so a client that always sends a {@code CONTENT} sends {@code {}} for an edge without data, as
   * openCypher {@code CREATE (a)-[:K {}]->(b)} and the batch loader accept the same empty map (issue #12871).
   */
  @Override
  public MutableEdge fromJSON(final JSONObject json) {
    if (!json.isEmpty())
      throw cannotHaveProperties();
    return this;
  }

  @Override
  public void merge(final Map<String, Object> other) {
    if (!other.isEmpty())
      throw cannotHaveProperties();
  }

  private IllegalArgumentException cannotHaveProperties() {
    return GraphEngine.lightweightEdgeCannotHaveProperties(getTypeName());
  }
}
