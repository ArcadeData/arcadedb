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
package com.arcadedb.bolt.structure;

import com.arcadedb.bolt.packstream.PackStreamStructure;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;
import java.util.Map;

/**
 * BOLT Relationship structure representing a graph edge.
 * Structure signature: 0x52
 * Fields (BOLT v4.x): id, startNodeId, endNodeId, type, properties
 * Note: element_id fields were added in BOLT v5.0, but we use v4.x format for compatibility
 */
public class BoltRelationship implements PackStreamStructure {
  public static final byte SIGNATURE = 0x52;

  private final long                id;
  private final long                startNodeId;
  private final long                endNodeId;
  private final String              type;
  private final Map<String, Object> properties;
  private final String              elementId;
  private final String              startNodeElementId;
  private final String              endNodeElementId;

  public BoltRelationship(final long id, final long startNodeId, final long endNodeId, final String type,
      final Map<String, Object> properties, final String elementId, final String startNodeElementId,
      final String endNodeElementId) {
    this.id = id;
    this.startNodeId = startNodeId;
    this.endNodeId = endNodeId;
    this.type = type;
    this.properties = properties != null ? properties : Map.of();
    this.elementId = elementId != null ? elementId : String.valueOf(id);
    this.startNodeElementId = startNodeElementId != null ? startNodeElementId : String.valueOf(startNodeId);
    this.endNodeElementId = endNodeElementId != null ? endNodeElementId : String.valueOf(endNodeId);
  }

  @Override
  public byte getSignature() {
    return SIGNATURE;
  }

  @Override
  public int getFieldCount() {
    return 5; // BOLT v4.x format
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    // Use BOLT v4.x format with 5 fields for compatibility
    writer.writeStructureHeader(SIGNATURE, 5);
    writer.writeInteger(id);
    writer.writeInteger(startNodeId);
    writer.writeInteger(endNodeId);
    writer.writeString(type);
    writer.writeMap(properties);
    // Note: element_id fields are omitted for v4.x compatibility
  }

  public long getId() {
    return id;
  }

  public long getStartNodeId() {
    return startNodeId;
  }

  public long getEndNodeId() {
    return endNodeId;
  }

  public String getType() {
    return type;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public String getElementId() {
    return elementId;
  }

  public String getStartNodeElementId() {
    return startNodeElementId;
  }

  public String getEndNodeElementId() {
    return endNodeElementId;
  }

  @Override
  public String toString() {
    return "Relationship{id=" + id + ", startNodeId=" + startNodeId + ", endNodeId=" + endNodeId + ", type=" + type
        + ", properties=" + properties + "}";
  }
}
