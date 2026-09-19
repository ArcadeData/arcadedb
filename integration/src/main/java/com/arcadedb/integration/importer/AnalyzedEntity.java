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
  public enum EntityType {DATABASE, DOCUMENT, VERTEX, EDGE}

  private final String                        name;
  private final EntityType                    type;
  private final Map<String, AnalyzedProperty> properties;
  private final long                          maxValueSampling;
  private       long                          totalRowLength = 0;
  private       long                          analyzedRows   = 0;
  /**
   * How many columns the source's header declares, or {@code -1} when the source has no notion of one.
   * <p>
   * Recorded by the analysis so the LOAD pass can measure a row against the same number the analysis measured it
   * against, instead of rediscovering it from the properties it happened to create - which is not the same number:
   * a property only exists once some row supplied a value at its index, so a header column no row ever fills leaves
   * no trace in {@link #properties} at all (issue #7782).
   */
  private       int                           headerColumns  = -1;

  public AnalyzedEntity(final String name, final EntityType type, final long maxValueSampling) {
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

  /** See {@link #headerColumns}. Ignores a non-positive count: "no header" stays "no header". */
  public void setHeaderColumns(final int headerColumns) {
    if (headerColumns > 0)
      this.headerColumns = headerColumns;
  }

  public int getHeaderColumns() {
    return headerColumns;
  }

  /**
   * The mean length of the rows the analysis measured, or {@code 0} when it measured none.
   * <p>
   * The zero case is not hypothetical any more: {@link #setRowSize} is what increments {@code analyzedRows}, and a
   * ragged row the analysis refuses never reaches it, so an entity can exist with no measured row at all (issue
   * #7782). This used to divide by {@code analyzedRows} unguarded and would have thrown {@code ArithmeticException}
   * in place of the row-shape diagnosis its caller was on its way to report.
   * <p>
   * Guarding here was NOT enough on its own, and the pairing is the part worth keeping straight. The one caller -
   * the {@code expectedEdges} batch estimate in {@code CSVImporterFormat.loadEdges()} - divides a {@code long} by
   * this {@code int}, so handing it a zero only moved the same {@code ArithmeticException} to the division at the
   * call site. That caller therefore gates on a measured average of its own ({@code averageRowLength > 0}) and skips
   * the division entirely, leaving its {@code expectedEdges} at zero for its own {@code expectedEdges <= 0} branch
   * to answer - which is exactly the "no idea how big this source is" case. Change either side and the other stops
   * making sense.
   */
  public int getAverageRowLength() {
    return analyzedRows > 0 ? (int) (totalRowLength / analyzedRows) : 0;
  }

  public void setRowSize(final String[] row) {
    for (String s : row) {
      if (s != null)
        totalRowLength += s.length();

      ++totalRowLength; // Delimiter
    }
    ++totalRowLength; // ADD LF

    ++analyzedRows;
  }

  public String getName() {
    return name;
  }

  public EntityType getType() {
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
