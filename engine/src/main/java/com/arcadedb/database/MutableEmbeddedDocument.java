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
  public synchronized MutableEmbeddedDocument save() {
    ((MutableDocument) modifier.getOwner()).save();
    return this;
  }

  @Override
  public synchronized MutableEmbeddedDocument save(final String bucketName) {
    ((MutableDocument) modifier.getOwner()).save(bucketName);
    return this;
  }

  @Override
  public synchronized void reload() {
    modifier.getOwner().reload();
  }

  @Override
  public synchronized MutableEmbeddedDocument modify() {
    return this;
  }

  @Override
  public byte getRecordType() {
    return EmbeddedDocument.RECORD_TYPE;
  }

  @Override
  public boolean equals(final Object o) {
    return ImmutableEmbeddedDocument.equals(this, o);
  }

  @Override
  public int hashCode() {
    return ImmutableEmbeddedDocument.hashCode(this);
  }
}
