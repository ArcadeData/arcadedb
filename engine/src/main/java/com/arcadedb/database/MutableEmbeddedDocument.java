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

package com.arcadedb.database;

import com.arcadedb.schema.DocumentType;

public class MutableEmbeddedDocument extends MutableDocument implements EmbeddedDocument {
  private final EmbeddedModifier modifier;

  /**
   * Creation constructor.
   */
  public MutableEmbeddedDocument(final Database db, final DocumentType type, final EmbeddedModifier modifier) {
    super(db, type, null);
    this.modifier = modifier;
  }

  /**
   * Copy constructor from ImmutableVertex.modify().
   */
  public MutableEmbeddedDocument(final Database db, final DocumentType type, final Binary buffer, final EmbeddedModifier modifier) {
    super(db, type, null, buffer);
    this.modifier = modifier;
  }

  @Override
  public MutableEmbeddedDocument save() {
    ((MutableDocument) modifier.getOwner()).save();
    return this;
  }

  @Override
  public MutableEmbeddedDocument save(final String bucketName) {
    ((MutableDocument) modifier.getOwner()).save(bucketName);
    return this;
  }

  @Override
  public void reload() {
    modifier.getOwner().reload();
  }

  @Override
  public MutableEmbeddedDocument modify() {
    return this;
  }

  @Override
  public byte getRecordType() {
    return EmbeddedDocument.RECORD_TYPE;
  }
}
