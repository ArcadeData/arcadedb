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
import com.arcadedb.database.DetachedDocument;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;

import java.math.*;
import java.time.*;
import java.util.*;

/**
 * Vertex wrapper to share a vertex among threads. This is useful when you want to cache the vertex in RAM to
 * be used by multiple threads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see MutableVertex
 * @see ImmutableVertex
 */
public class SynchronizedVertex implements Vertex {
  private final Vertex delegate;

  public SynchronizedVertex(final Vertex delegate) {
    this.delegate = delegate;
  }

  @Override
  public synchronized byte getRecordType() {
    return delegate.getRecordType();
  }

  @Override
  public synchronized MutableVertex modify() {
    return delegate.modify();
  }

  @Override
  public synchronized void reload() {
    delegate.reload();
  }

  @Override
  public synchronized MutableEdge newEdge(String edgeType, Identifiable toVertex, Object... properties) {
    return delegate.newEdge(edgeType, toVertex, properties);
  }

  @Deprecated
  @Override
  public synchronized MutableEdge newEdge(String edgeType, Identifiable toVertex, boolean bidirectional, Object... properties) {
    return delegate.newEdge(edgeType, toVertex, bidirectional, properties);
  }

  @Override
  public synchronized ImmutableLightEdge newLightEdge(String edgeType, Identifiable toVertex) {
    return delegate.newLightEdge(edgeType, toVertex);
  }

  @Deprecated
  @Override
  public synchronized ImmutableLightEdge newLightEdge(String edgeType, Identifiable toVertex, boolean bidirectional) {
    return delegate.newLightEdge(edgeType, toVertex, bidirectional);
  }

  @Override
  public synchronized long countEdges(final DIRECTION direction, final String edgeType) {
    return delegate.countEdges(direction, edgeType);
  }

  @Override
  public synchronized Iterable<Edge> getEdges() {
    return delegate.getEdges();
  }

  @Override
  public synchronized Iterable<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return delegate.getEdges(direction, edgeTypes);
  }

  @Override
  public synchronized Iterable<Vertex> getVertices() {
    return delegate.getVertices();
  }

  @Override
  public synchronized Iterable<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
    return delegate.getVertices(direction, edgeTypes);
  }

  @Override
  public synchronized boolean isConnectedTo(final Identifiable toVertex) {
    return delegate.isConnectedTo(toVertex);
  }

  @Override
  public synchronized boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return delegate.isConnectedTo(toVertex, direction);
  }

  @Override
  public synchronized boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction, final String edgeType) {
    return delegate.isConnectedTo(toVertex, direction, edgeType);
  }

  @Override
  public synchronized RID moveToType(String targetType) {
    return delegate.moveToType(targetType);
  }

  @Override
  public synchronized RID moveToBucket(String targetBucket) {
    return delegate.moveToBucket(targetBucket);
  }

  @Override
  public synchronized Vertex asVertex() {
    return delegate.asVertex();
  }

  @Override
  public synchronized Vertex asVertex(boolean loadContent) {
    return delegate.asVertex(loadContent);
  }

  @Override
  public synchronized Map<String, Object> toMap(final boolean includeMetadata) {
    return delegate.toMap(includeMetadata);
  }

  @Override
  public synchronized JSONObject toJSON(final boolean includeMetadata) {
    return delegate.toJSON(includeMetadata);
  }

  @Override
  public synchronized boolean has(String propertyName) {
    return delegate.has(propertyName);
  }

  @Override
  public synchronized Object get(String propertyName) {
    return delegate.get(propertyName);
  }

  @Override
  public synchronized Map<String, Object> propertiesAsMap() {
    return delegate.propertiesAsMap();
  }

  @Override
  public synchronized Map<String, Object> toMap() {
    return delegate.toMap();
  }

  @Override
  public synchronized String toString() {
    return delegate.toString();
  }

  @Override
  public synchronized Set<String> getPropertyNames() {
    return delegate.getPropertyNames();
  }

  @Override
  public synchronized Document asDocument() {
    return delegate.asDocument();
  }

  @Override
  public synchronized Document asDocument(boolean loadContent) {
    return delegate.asDocument(loadContent);
  }

  @Override
  public synchronized DetachedDocument detach() {
    return delegate.detach();
  }

  @Override
  public synchronized DetachedDocument detach(boolean filterHiddenProperties) {
    return delegate.detach(filterHiddenProperties);
  }

  @Override
  public synchronized String getString(String propertyName) {
    return delegate.getString(propertyName);
  }

  @Override
  public synchronized Boolean getBoolean(String propertyName) {
    return delegate.getBoolean(propertyName);
  }

  @Override
  public synchronized Byte getByte(String propertyName) {
    return delegate.getByte(propertyName);
  }

  @Override
  public synchronized Short getShort(String propertyName) {
    return delegate.getShort(propertyName);
  }

  @Override
  public synchronized Integer getInteger(String propertyName) {
    return delegate.getInteger(propertyName);
  }

  @Override
  public synchronized Long getLong(String propertyName) {
    return delegate.getLong(propertyName);
  }

  @Override
  public synchronized Float getFloat(String propertyName) {
    return delegate.getFloat(propertyName);
  }

  @Override
  public synchronized Double getDouble(String propertyName) {
    return delegate.getDouble(propertyName);
  }

  @Override
  public synchronized BigDecimal getDecimal(String propertyName) {
    return delegate.getDecimal(propertyName);
  }

  @Override
  public synchronized Date getDate(String propertyName) {
    return delegate.getDate(propertyName);
  }

  @Override
  public synchronized Calendar getCalendar(String propertyName) {
    return delegate.getCalendar(propertyName);
  }

  @Override
  public synchronized LocalDate getLocalDate(String propertyName) {
    return delegate.getLocalDate(propertyName);
  }

  @Override
  public synchronized LocalDateTime getLocalDateTime(String propertyName) {
    return delegate.getLocalDateTime(propertyName);
  }

  @Override
  public synchronized ZonedDateTime getZonedDateTime(String propertyName) {
    return delegate.getZonedDateTime(propertyName);
  }

  @Override
  public synchronized Instant getInstant(String propertyName) {
    return delegate.getInstant(propertyName);
  }

  @Override
  public synchronized byte[] getBinary(String propertyName) {
    return delegate.getBinary(propertyName);
  }

  @Override
  public synchronized Map<String, Object> getMap(String propertyName) {
    return delegate.getMap(propertyName);
  }

  @Override
  public synchronized <T> List<T> getList(String propertyName) {
    return delegate.getList(propertyName);
  }

  @Override
  public synchronized EmbeddedDocument getEmbedded(String propertyName) {
    return delegate.getEmbedded(propertyName);
  }

  @Override
  public synchronized DocumentType getType() {
    return delegate.getType();
  }

  @Override
  public synchronized String getTypeName() {
    return delegate.getTypeName();
  }

  @Override
  public synchronized JSONObject toJSON(String... includeProperties) {
    return delegate.toJSON(includeProperties);
  }

  @Override
  public synchronized RID getIdentity() {
    return delegate.getIdentity();
  }

  @Override
  public synchronized Record getRecord() {
    return delegate.getRecord();
  }

  @Override
  public synchronized Record getRecord(boolean loadContent) {
    return delegate.getRecord(loadContent);
  }

  @Override
  public synchronized int size() {
    return delegate.size();
  }

  @Override
  public synchronized void delete() {
    delegate.delete();
  }

  @Override
  public synchronized boolean equals(final Object o) {
    return delegate.equals(o);
  }

  @Override
  public synchronized int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public synchronized Database getDatabase() {
    return delegate.getDatabase();
  }

  @Override
  public synchronized Edge asEdge() {
    return delegate.asEdge();
  }

  @Override
  public synchronized Edge asEdge(boolean loadContent) {
    return delegate.asEdge(loadContent);
  }

  @Override
  public synchronized JSONObject toJSON() {
    return delegate.toJSON();
  }
}
