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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;

import java.util.Iterator;

/**
 * Refuses to declare a constraint over records that already violate it (#8943, #9112). A record written before the
 * constraint stays legal, yet it can no longer take any update, and the planners trust MANDATORY + NOTNULL to mean "an
 * index on the property holds every record", so such a record silently vanishes from an index-ordered read.
 * <p>
 * Each check reads the whole type, polymorphically, and stops at the first offender, naming its RID. Best effort: not
 * atomic with the schema change (CREATE PROPERTY creates the property first and drops it again if the scan refuses it, so for that
 * window concurrent writers already see the new constraints), so a writer racing the DDL can still slip a record in, and the Java schema API
 * ({@code Property.setMandatory()}) is not guarded at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ExistingRecordsCheck {
  private ExistingRecordsCheck() {
  }

  /**
   * @param mandatory true when MANDATORY is about to be declared
   * @param notNull   true when NOTNULL is about to be declared
   */
  public static void requireExistence(final Database db, final DocumentType type, final String propertyName,
      final boolean mandatory, final boolean notNull) {
    requireExistence(db, type, new String[] { propertyName }, mandatory, notNull);
  }

  /**
   * The same check for several properties in ONE pass over the type (a composite NODE KEY).
   */
  public static void requireExistence(final Database db, final DocumentType type, final String[] propertyNames,
      final boolean mandatory, final boolean notNull) {
    if (propertyNames.length == 0)
      return;
    final Iterator<Record> records = db.iterateType(type.getName(), true);
    while (records.hasNext()) {
      if (!(records.next() instanceof Document document))
        continue;
      for (final String propertyName : propertyNames) {
        final DocumentValidator.ExistenceConstraint unmet = DocumentValidator.unmetExistenceConstraint(document, propertyName,
            mandatory, notNull);
        if (unmet != null)
          throw new CommandExecutionException("Cannot set " + (unmet == DocumentValidator.ExistenceConstraint.MANDATORY ?
              "MANDATORY" :
              "NOTNULL") + ": record " + document.getIdentity() + " violates it, "
              + DocumentValidator.describeUnmetExistenceConstraint(document, propertyName, unmet)
              + ". Fix or delete the non-conforming records first");
      }
    }
  }

  /**
   * @param what what is being declared, for the message (e.g. {@code "MAX 20"} or {@code "property"})
   */
  public static void requireValues(final Database db, final DocumentType type, final Property property,
      final DocumentValidator.StoredValueConstraints constraints, final String what) {
    final Iterator<Record> records = db.iterateType(type.getName(), true);
    while (records.hasNext()) {
      if (!(records.next() instanceof Document document))
        continue;
      try {
        DocumentValidator.validateStoredValue(document, property, constraints);
      } catch (final ValidationException e) {
        throw new CommandExecutionException(
            "Cannot set " + what + ": record " + document.getIdentity() + " violates it, " + e.getMessage()
                + ". Fix or delete the non-conforming records first", e);
      }
    }
  }

  /**
   * The whole declaration of a property just created: existence flags, declared type, MIN, MAX and REGEXP, in one pass over
   * the type.
   */
  public static void requireDeclaration(final Database db, final DocumentType type, final Property property) {
    final DocumentValidator.StoredValueConstraints constraints = DocumentValidator.StoredValueConstraints.of(db, property);
    // Nothing a stored record holds can violate this declaration (e.g. a plain STRING with no flags): skip the full read
    if (!property.isMandatory() && !property.isNotNull() && !constraints.canBeViolatedOn(property))
      return;

    final Iterator<Record> records = db.iterateType(type.getName(), true);
    while (records.hasNext()) {
      if (!(records.next() instanceof Document document))
        continue;
      final DocumentValidator.ExistenceConstraint unmet = DocumentValidator.unmetExistenceConstraint(document, property);
      if (unmet != null)
        throw new CommandExecutionException(
            createRefusal(type, property, document, DocumentValidator.describeUnmetExistenceConstraint(document, property)));
      try {
        DocumentValidator.validateStoredValue(document, property, constraints);
      } catch (final ValidationException e) {
        throw new CommandExecutionException(createRefusal(type, property, document, e.getMessage()), e);
      }
    }
  }

  private static String createRefusal(final DocumentType type, final Property property, final Document document,
      final String reason) {
    return "Cannot create property '" + type.getName() + "." + property.getName() + "': record " + document.getIdentity()
        + " violates it, " + reason + ". Fix or delete the non-conforming records first";
  }
}
