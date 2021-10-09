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
 */
package com.arcadedb.server.ha;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

public class ReplicatedDocumentType extends DocumentType {
  public ReplicatedDocumentType(EmbeddedSchema schema, String name) {
    super(schema, name);
  }

  @Override
  public Property createProperty(String propertyName, Type propertyType) {
    return (Property) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.createProperty(propertyName, propertyType));
  }

  @Override
  public Property createProperty(String propertyName, Class<?> propertyType) {
    return (Property) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.createProperty(propertyName, propertyType));
  }

  @Override
  public Property createProperty(String propertyName, String propertyType) {
    return (Property) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.createProperty(propertyName, propertyType));
  }

  @Override
  public Property getOrCreateProperty(String propertyName, Type propertyType) {
    return (Property) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.getOrCreateProperty(propertyName, propertyType));
  }

  @Override
  public Property getOrCreateProperty(String propertyName, Class<?> propertyType) {
    return (Property) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.getOrCreateProperty(propertyName, propertyType));
  }

  @Override
  public Property getOrCreateProperty(String propertyName, String propertyType) {
    return (Property) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.getOrCreateProperty(propertyName, propertyType));
  }

  @Override
  public Index createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String... propertyNames) {
    return (Index) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.createTypeIndex(indexType, unique, propertyNames));
  }

  @Override
  public Index createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize) {
    return (Index) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.createTypeIndex(indexType, unique, propertyNames, pageSize));
  }

  @Override
  public Index createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize, Index.BuildIndexCallback callback) {
    return (Index) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.createTypeIndex(indexType, unique, propertyNames, pageSize, callback));
  }

  @Override
  public Index createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback) {
    return (Index) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.createTypeIndex(indexType, unique, propertyNames, pageSize, nullStrategy, callback));
  }

  @Override
  public Index getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String... propertyNames) {
    return (Index) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.getOrCreateTypeIndex(indexType, unique, propertyNames));
  }

  @Override
  public Index getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize) {
    return (Index) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.getOrCreateTypeIndex(indexType, unique, propertyNames, pageSize));
  }

  @Override
  public Index getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback) {
    return (Index) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.getOrCreateTypeIndex(indexType, unique, propertyNames, pageSize, callback));
  }

  @Override
  public Index getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback) {
    return (Index) ((ReplicatedDatabase) ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()).recordFileChanges(
        () -> super.getOrCreateTypeIndex(indexType, unique, propertyNames, pageSize, nullStrategy, callback));
  }
}
