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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.*;

public class LocalProperty extends AbstractProperty {

  public LocalProperty(final LocalDocumentType owner, final String name, final Type type) {
    super(owner, name, type, owner.getSchema().getDictionary().getIdByName(name, true));
  }

  /**
   * Enforces the UPDATE_SCHEMA permission for any property-level schema mutation. No-op in embedded mode or when no
   * current user is bound to the thread (e.g. schema load at startup, replication apply).
   */
  private void checkForSchemaMutation() {
    ((DatabaseInternal) owner.getSchema().getEmbedded().getDatabase())
        .checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
  }

  /**
   * A {@link Property} object outlives the DROP PROPERTY that removed it, so a caller can hold one that the type no
   * longer declares. Writing a default through such a handle would put the dropped name back into the type's
   * default-property cache and break the next record create - issue #6799 reached from the other side. Identity and
   * not mere presence, so a namesake recreated in between is not written through the stale handle either.
   */
  private void checkStillDeclaredIn(final LocalDocumentType ownerType) {
    if (ownerType.getPropertyIfExists(name) != this)
      throw new SchemaException("Cannot set the default value of property '" + ownerType.getName() + "." + name
          + "' because the property is no longer declared in the type");
  }

  @Override
  public Property setDefaultValue(final Object defaultValue) {
    checkForSchemaMutation();
    final LocalDocumentType ownerType = (LocalDocumentType) owner;

    // Whether the requested default happens to match the current one is not part of the caller's contract, so the
    // refusal cannot live only on the path that publishes something: a detached handle is refused first, whatever it
    // carries. This read is outside the schema write lock, which is all a request that writes nothing needs - the
    // authoritative check is the one inside the publication below.
    checkStillDeclaredIn(ownerType);

    final Database database = owner.getSchema().getEmbedded().getDatabase();

    final Object convertedValue = defaultValue instanceof String ?
        defaultValue :
        Type.convert(database, defaultValue, type.javaDefaultType);

    // Compiled once here rather than on every record create, and before any state is touched, so a rejected default
    // leaves the property exactly as it was. Before the "did it change?" check and not inside it: a default persisted
    // by an earlier release is loaded without validation, so re-setting one to the same invalid text is the one case
    // where new == old and the caller still has to be told it is invalid.
    final Expression compiled = compileDefaultValue(convertedValue, database);

    if (!Objects.equals(this.defaultValue.value(), convertedValue)) {
      // The property's own default and the owner type's default-property cache are two views of one fact, and the
      // other statement that changes them, DROP PROPERTY, mutates both under the schema write lock (recordFileChanges).
      // Publishing them outside that lock let a concurrent DROP PROPERTY on the same type interleave between the two
      // and leave the cache naming a property that no longer exists - the very state issue #6799 is about. Only the
      // publication is serialized: conversion and validation stay outside, so a rejected default touches no state and
      // its SchemaException reaches the caller unwrapped, and the write lock is held for two field assignments.
      ownerType.recordFileChanges(() -> {
        // Re-checked here because this is the check that counts: holding the same lock as dropProperty() settles the
        // order of the two, so whoever arrives second sees the other's result rather than a snapshot taken before it.
        checkStillDeclaredIn(ownerType);

        // One publication, so no reader can see the new value with the old (or no) compiled expression.
        this.defaultValue = new DefaultValue(convertedValue, compiled);
        ownerType.setPropertyHasDefault(name, convertedValue != DEFAULT_NOT_SET);
        return null;
      });
    }
    return this;
  }

  @Override
  public Property setOfType(String ofType) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.ofType, ofType);
    if (changed) {
      final LocalSchema schema = (LocalSchema) owner.getSchema();
      if (type == Type.LIST || type == Type.MAP) {
        if (Type.getTypeByName(ofType) != null) {
          ofType = ofType.toUpperCase(Locale.ENGLISH);
        } else {
          if (!schema.existsType(ofType))
            throw new SchemaException("Error on set property " + owner.getName() + "." + name + " " + type + " type to '" + ofType
                + "' because the type is not defined");
        }
      } else if (type == Type.LINK || type == Type.EMBEDDED) {
        if (schema.isSchemaLoaded() && !schema.existsType(ofType))
          throw new SchemaException("Error on set property " + owner.getName() + "." + name + " " + type + " type to '" + ofType
              + "' because the type is not defined");
      }

      this.ofType = ofType;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setReadonly(final boolean readonly) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.readonly, readonly);
    if (changed) {
      this.readonly = readonly;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setMandatory(final boolean mandatory) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.mandatory, mandatory);
    if (changed) {
      this.mandatory = mandatory;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  /**
   * Sets the constraint `not null` for the current property. If true, the property cannot be null.
   */
  @Override
  public Property setNotNull(final boolean notNull) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.notNull, notNull);
    if (changed) {
      this.notNull = notNull;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setHidden(final boolean hidden) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.hidden, hidden);
    if (changed) {
      this.hidden = hidden;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setExternal(final boolean external) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.external, external);
    if (changed) {
      final LocalDocumentType localOwner = (LocalDocumentType) owner;
      if (external) {
        // ORDERING IS LOAD-BEARING: paired buckets must exist BEFORE any reader can observe
        // ownExternalPropertyCount > 0. hasExternalProperties() short-circuits on the counter, and the
        // serializer's write path uses that to decide whether to route a value through the external bucket.
        // If we incremented first and another thread saw count > 0 before ensureExternalBucketsRecursive()
        // ran, it could try to write into a bucket that doesn't exist yet. Schema mutations are typically
        // serialised by the schema lock, but this ordering keeps the property setter safe even without it.
        localOwner.ensureExternalBucketsRecursive();
        this.external = external;
        localOwner.ownExternalPropertyCount.incrementAndGet();
      } else {
        // Flip the flag first so concurrent serialisations stop writing externally; the counter is the gate
        // hasExternalProperties() consults, so decrement last to keep the "count > 0 implies bucket exists"
        // invariant directional.
        this.external = external;
        localOwner.ownExternalPropertyCount.decrementAndGet();
      }
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setCompression(final String compression) {
    checkForSchemaMutation();
    final String normalized;
    if (compression == null || compression.isEmpty() || "none".equalsIgnoreCase(compression))
      normalized = null;
    else if ("fast".equalsIgnoreCase(compression) || "max".equalsIgnoreCase(compression)
        || "auto".equalsIgnoreCase(compression))
      normalized = compression.toLowerCase(Locale.ENGLISH);
    else if ("lz4".equalsIgnoreCase(compression))
      // Backwards alias: "lz4" was the original name for the fast tier; map it to "fast" so existing schema
      // configs keep working without touching the schema.json on disk.
      normalized = "fast";
    else
      throw new IllegalArgumentException(
          "Unsupported compression '" + compression + "' (supported: none, fast, max, auto)");
    if (!Objects.equals(this.compression, normalized)) {
      this.compression = normalized;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setMax(final String max) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.max, max);
    if (changed) {
      switch (type) {
      case LINK:
      case BOOLEAN:
      case EMBEDDED:
        throw new IllegalArgumentException("Maximum value not applicable for type " + type);

      case STRING:
      case BINARY:
      case LIST:
      case MAP:
        if (Integer.parseInt(max) < 0)
          throw new IllegalArgumentException("Maximum value for type " + type + " is 0");
        break;
      }

      this.max = max;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setMin(final String min) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.min, min);
    if (changed) {
      switch (type) {
      case LINK:
      case BOOLEAN:
      case EMBEDDED:
        throw new IllegalArgumentException("Minimum value not applicable for type " + type);

      case STRING:
      case BINARY:
      case LIST:
      case MAP:
        if (Integer.parseInt(min) < 0)
          throw new IllegalArgumentException("Minimum value for type " + type + " is 0");
        break;
      }

      this.min = min;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setRegexp(final String regexp) {
    checkForSchemaMutation();
    final boolean changed = !Objects.equals(this.regexp, regexp);
    if (changed) {
      this.regexp = regexp;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Object setCustomValue(final String key, final Object value) {
    checkForSchemaMutation();
    final Object prev;
    if (value == null)
      prev = custom.remove(key);
    else
      prev = custom.put(key, value);

    if (!Objects.equals(prev, value))
      owner.getSchema().getEmbedded().saveConfiguration();

    return prev;
  }
}
