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

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.DocumentType;

import java.util.Set;

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

  public synchronized MutableEmbeddedDocument modify() {
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

  @Override
  public boolean equals(final Object o) {
    return equals(this, o);
  }

  @Override
  public int hashCode() {
    return hashCode(this);
  }

  static boolean equals(final EmbeddedDocument me, final Object o) {
    if (me == o)
      return true;
    if (!(o instanceof Document))
      return false;
    final Document that = (Document) o;

    final Set<String> props = me.getPropertyNames();
    if (!props.equals(that.getPropertyNames()))
      return false;

    for (String prop : props) {
      final Object v1 = me.get(prop);
      final Object v2 = that.get(prop);

      if (v1 == null && v2 == null)
        continue;
      else if (v1 != null && !v1.equals(v2))
        return false;
    }

    return true;
  }

  static int hashCode(final EmbeddedDocument me) {
    int hash = 0;
    final Set<String> props = me.getPropertyNames();
    for (String prop : props) {
      final Object value = me.get(prop);
      hash += value != null ? value.hashCode() : 0;
    }
    return hash;
  }
}
