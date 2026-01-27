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
import java.util.List;
import java.util.Map;

/**
 * BOLT Node structure representing a graph vertex.
 * Structure signature: 0x4E
 * Fields (BOLT v4.x): id (Integer), labels (List<String>), properties (Map)
 * Note: element_id was added in BOLT v5.0, but we use v4.x format for compatibility
 */
public class BoltNode implements PackStreamStructure {
  public static final byte SIGNATURE = 0x4E;

  private final long                id;
  private final List<String>        labels;
  private final Map<String, Object> properties;
  private final String              elementId;

  public BoltNode(final long id, final List<String> labels, final Map<String, Object> properties, final String elementId) {
    this.id = id;
    this.labels = labels != null ? labels : List.of();
    this.properties = properties != null ? properties : Map.of();
    this.elementId = elementId != null ? elementId : String.valueOf(id);
  }

  @Override
  public byte getSignature() {
    return SIGNATURE;
  }

  @Override
  public int getFieldCount() {
    return 3; // id, labels, properties (v4.x format)
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    // Use BOLT v4.x format with 3 fields for compatibility
    writer.writeStructureHeader(SIGNATURE, 3);
    writer.writeInteger(id);
    writer.writeList(labels);
    writer.writeMap(properties);
    // Note: element_id is omitted for v4.x compatibility
  }

  public long getId() {
    return id;
  }

  public List<String> getLabels() {
    return labels;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public String getElementId() {
    return elementId;
  }

  @Override
  public String toString() {
    return "Node{id=" + id + ", labels=" + labels + ", properties=" + properties + ", elementId=" + elementId + "}";
  }
}
