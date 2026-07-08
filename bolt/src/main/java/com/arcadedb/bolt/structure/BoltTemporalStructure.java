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

/**
 * A Bolt temporal PackStream structure (Date, Time, LocalTime, LocalDateTime, DateTime,
 * DateTimeZoneId) emitted on the outbound path so a Neo4j-compatible client receives a native
 * temporal value instead of an ISO-8601 string. The signature and pre-computed fields are built by
 * {@link BoltStructureMapper}; the fields are {@code Long}/{@code String} values the writer already
 * knows how to serialize.
 */
public class BoltTemporalStructure implements PackStreamStructure {
  private final byte     signature;
  private final Object[] fields;

  public BoltTemporalStructure(final byte signature, final Object... fields) {
    // No defensive copy: every call site passes a freshly-allocated varargs array and the array is never
    // exposed (writeTo reads it directly, there is no getter), so a copy would only add per-value GC churn
    // on the serialization hot path.
    this.signature = signature;
    this.fields = fields != null ? fields : new Object[0];
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, fields.length);
    for (final Object field : fields)
      writer.writeValue(field);
  }
}
