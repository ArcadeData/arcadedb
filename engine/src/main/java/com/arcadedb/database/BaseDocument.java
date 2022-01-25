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
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JavaBinarySerializer;

import java.io.*;
import java.math.BigDecimal;
import java.util.Date;

public abstract class BaseDocument extends BaseRecord implements Document, Serializable, Externalizable {
  protected final DocumentType type;
  protected       int          propertiesStartingPosition = 1;

  protected BaseDocument(final Database database, final DocumentType type, final RID rid, final Binary buffer) {
    super(database, rid, buffer);
    this.type = type;
  }

  @Override
  public Document asDocument() {
    return this;
  }

  @Override
  public Document asDocument(final boolean loadContent) {
    return this;
  }

  public DetachedDocument detach() {
    return new DetachedDocument(this);
  }

  @Override
  public String getString(final String propertyName) {
    return (String) Type.convert(database, get(propertyName), String.class);
  }

  @Override
  public Boolean getBoolean(final String propertyName) {
    return (Boolean) Type.convert(database, get(propertyName), Boolean.class);
  }

  @Override
  public Byte getByte(final String propertyName) {
    return (Byte) Type.convert(database, get(propertyName), Byte.class);
  }

  @Override
  public Short getShort(final String propertyName) {
    return (Short) Type.convert(database, get(propertyName), Short.class);
  }

  @Override
  public Integer getInteger(final String propertyName) {
    return (Integer) Type.convert(database, get(propertyName), Integer.class);
  }

  @Override
  public Long getLong(final String propertyName) {
    return (Long) Type.convert(database, get(propertyName), Long.class);
  }

  @Override
  public Float getFloat(final String propertyName) {
    return (Float) Type.convert(database, get(propertyName), Float.class);
  }

  @Override
  public Double getDouble(final String propertyName) {
    return (Double) Type.convert(database, get(propertyName), Double.class);
  }

  @Override
  public BigDecimal getDecimal(final String propertyName) {
    return (BigDecimal) Type.convert(database, get(propertyName), BigDecimal.class);
  }

  @Override
  public Date getDate(final String propertyName) {
    return (Date) Type.convert(database, get(propertyName), Date.class);
  }

  @Override
  public EmbeddedDocument getEmbedded(final String propertyName) {
    return (EmbeddedDocument) Type.convert(database, get(propertyName), EmbeddedDocument.class);
  }

  public DocumentType getType() {
    return type;
  }

  public String getTypeName() {
    return type.getName();
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  @Override
  public void reload() {
    super.reload();
    if (buffer != null)
      buffer.position(propertiesStartingPosition);
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    JavaBinarySerializer.writeExternal(this, out);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    JavaBinarySerializer.readExternal(this, in);
  }
}
