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

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.DocumentType;

public class ImmutableEmbeddedDocument extends ImmutableDocument implements EmbeddedDocument {
  private final EmbeddedModifier modifier;

  protected ImmutableEmbeddedDocument(final Database database, final DocumentType type, final Binary buffer, final EmbeddedModifier owner) {
    super(database, type, null, buffer);
    this.modifier = owner;
  }

  @Override
  public byte getRecordType() {
    return EmbeddedDocument.RECORD_TYPE;
  }

  public MutableEmbeddedDocument modify() {
    if (modifier == null)
      throw new DatabaseOperationException("Cannot modify a detached embedded record");

    modifier.getOwner().modify();
    final EmbeddedDocument mostRecent = modifier.getEmbeddedDocument();

    if (mostRecent == this) {
      // CURRENT RECORD IS THE MOST RECENT IN TX, CREATE A MUTABLE DOC, REPLACE ITSELF IN THE OWNER DOCUMENT AND RETURN IT
      checkForLazyLoading();
      buffer.rewind();
      MutableEmbeddedDocument newRecord = new MutableEmbeddedDocument(database, type, buffer.copy(), modifier);
      modifier.setEmbeddedDocument(newRecord);
      return newRecord;
    } else if (mostRecent instanceof MutableEmbeddedDocument) {
      // A MUTABLE VERSION IS ALREADY ATTACHED TO THE OWNER, USE THIS
      return (MutableEmbeddedDocument) mostRecent;
    }

    // NEWEST IMMUTABLE VERSION, DELEGATE THE MODIFY TO THIS
    return (MutableEmbeddedDocument) mostRecent.modify();
  }
}
