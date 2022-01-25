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
package com.arcadedb.integration.importer;

import com.arcadedb.schema.Type;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class AnalyzedEntity {
  public enum ENTITY_TYPE {DATABASE, DOCUMENT, VERTEX, EDGE}

  private final String                        name;
  private final ENTITY_TYPE                   type;
  private final Map<String, AnalyzedProperty> properties;
  private       long                          totalRowLength = 0;
  private       long analyzedRows   = 0;
  private final long maxValueSampling;

  public AnalyzedEntity(final String name, final ENTITY_TYPE type, final long maxValueSampling) {
    this.name = name;
    this.type = type;
    this.properties = new LinkedHashMap<>();
    this.maxValueSampling = maxValueSampling;
  }

  public Collection<AnalyzedProperty> getProperties() {
    return properties.values();
  }

  public AnalyzedProperty getProperty(final String name) {
    return properties.get(name);
  }

  public void getOrCreateProperty(final String name, final String content) {
    AnalyzedProperty property = properties.get(name);
    if (property == null) {
      property = new AnalyzedProperty(name, Type.STRING, maxValueSampling, properties.size());
      properties.put(property.getName(), property);
    }

    property.setLastContent(content);
  }

  public int getAverageRowLength() {
    return (int) (totalRowLength / analyzedRows);
  }

  public void setRowSize(final String[] row) {
    for (int i = 0; i < row.length; ++i) {
      totalRowLength += row[i].length() + 1;
    }
    ++totalRowLength; // ADD LF

    ++analyzedRows;
  }

  public String getName() {
    return name;
  }

  public ENTITY_TYPE getType() {
    return type;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final AnalyzedEntity that = (AnalyzedEntity) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return name;
  }
}
