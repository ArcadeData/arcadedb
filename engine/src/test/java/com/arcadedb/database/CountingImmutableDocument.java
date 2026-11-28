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
package com.arcadedb.database;

import com.arcadedb.schema.DocumentType;

import java.util.Map;
import java.util.Set;

/**
 * A lazily loaded {@link ImmutableDocument} that counts the reads that walk the record header: {@link #get(String)},
 * {@link #getIfPresent(String, Object)}, {@link #getPropertyNames()}, {@link #toMap(boolean)} and {@link #toMapReportingDamage(String[])} each deserialize from the first byte
 * of the header, so their sum is how many passes over the record a consumer paid (issue #10098).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CountingImmutableDocument extends ImmutableDocument {
  private int      selectiveMaps;
  private String[] lastSelectedNames;
  private int gets;
  private int names;
  private int maps;

  public CountingImmutableDocument(final Database database, final DocumentType type, final RID rid) {
    super(database, type, rid, null);
  }

  @Override
  public Object get(final String propertyName) {
    ++gets;
    return super.get(propertyName);
  }

  @Override
  public Object getIfPresent(final String propertyName, final Object absentValue) {
    ++gets;
    return super.getIfPresent(propertyName, absentValue);
  }

  @Override
  public Set<String> getPropertyNames() {
    ++names;
    return super.getPropertyNames();
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    ++maps;
    return super.toMap(includeMetadata);
  }

  @Override
  public Map<String, Object> toMapReportingDamage(final String[] damage) {
    ++maps;
    return super.toMapReportingDamage(damage);
  }

  @Override
  public Map<String, Object> propertiesAsMap(final String... fieldNames) {
    ++selectiveMaps;
    lastSelectedNames = fieldNames;
    return super.propertiesAsMap(fieldNames);
  }

  /** Reads of the whole record into a map, each decoding every value it holds. */
  public int wholeReads() {
    return maps;
  }

  /** Reads of the record restricted to some names, each decoding only the values of those names (issue #12558). */
  public int selectiveReads() {
    return selectiveMaps;
  }

  /** The names the last selective read asked for, or null if there was none. */
  public String[] lastSelectedNames() {
    return lastSelectedNames;
  }

  /** Reads of a single property by name, each a walk over the record header. */
  public int propertyReads() {
    return gets;
  }

  /** Every read that walked the record header: by name, the name list, the whole map and a selective map. */
  public int headerWalks() {
    return gets + names + maps + selectiveMaps;
  }

  public void resetCounters() {
    gets = 0;
    names = 0;
    maps = 0;
    selectiveMaps = 0;
    lastSelectedNames = null;
  }
}
