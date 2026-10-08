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
package com.arcadedb.graphql.schema;

import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.Type;

import java.util.Map;
import java.util.Optional;

import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphQLResult extends ResultInternal {
  RID                     identity;
  /** The row this one was projected from, which knows the declared type of the columns it read (issue #9468). */
  private Result              source;
  /** Response key to source field name, only for the aliased fields: null when no field is aliased. */
  private Map<String, String> aliases;

  public GraphQLResult() {
  }

  public GraphQLResult(final Map<String, Object> map) {
    super(map);
    identity = (RID) map.get(RID_PROPERTY);
  }

  /**
   * @param source  the row the fields were read from
   * @param aliases response key to field name for the aliased fields only, or null when none is aliased
   */
  GraphQLResult(final Map<String, Object> map, final Result source, final Map<String, String> aliases) {
    this(map);
    this.source = source;
    this.aliases = aliases;
  }

  /**
   * The property type of the column the field was read from, so the serializers format the value like they do for the
   * row it came from (a DATETIME_NANOS column keeps its nanosecond precision, issue #9468).
   */
  @Override
  public Type getPropertyType(final String name) {
    if (source == null)
      return super.getPropertyType(name);
    final String field = aliases != null ? aliases.get(name) : null;
    return source.getPropertyType(field != null ? field : name);
  }

  @Override
  public Optional<RID> getIdentity() {
    return identity == null ? Optional.empty() : Optional.of(identity);
  }
}
