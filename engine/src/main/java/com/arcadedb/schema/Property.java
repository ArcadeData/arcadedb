/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.schema;

import com.arcadedb.index.Index;

import java.util.Objects;

public class Property {
  private final DocumentType owner;
  private final String       name;
  private final Type         type;
  private final int          id;

  public Property(final DocumentType owner, final String name, final Type type) {
    this.owner = owner;
    this.name = name;
    this.type = type;
    this.id = owner.getSchema().getDictionary().getIdByName(name, true);
  }

  /**
   * Creates an index on this property.
   *
   * @param type   Index type between LSM_TREE and FULL_TEXT
   * @param unique true if the index is unique
   *
   * @return The index instance
   */
  public Index createIndex(final EmbeddedSchema.INDEX_TYPE type, final boolean unique) {
    return owner.createTypeIndex(type, unique, name);
  }

  /**
   * Returns an index on this property or creates it if not exists.
   *
   * @param type   Index type between LSM_TREE and FULL_TEXT
   * @param unique true if the index is unique
   *
   * @return The index instance
   */
  public Index getOrCreateIndex(final EmbeddedSchema.INDEX_TYPE type, final boolean unique) {
    return owner.getOrCreateTypeIndex(type, unique, name);
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public int getId() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final Property property = (Property) o;
    return id == property.id && Objects.equals(name, property.name) && Objects.equals(type, property.type);
  }

  @Override
  public int hashCode() {
    return id;
  }
}
