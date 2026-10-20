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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TimeSeriesColumnConstraints;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.TimeBoundRegex;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
    if (type instanceof LocalTimeSeriesType tsType) {
      for (final String propertyName : propertyNames)
        requireSamples(db, tsType, propertyName, mandatory || notNull, mandatory ? "MANDATORY" : "NOTNULL", null, null, null,
            mandatory ? "MANDATORY" : "NOTNULL");
      return;
    }
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
    if (type instanceof LocalTimeSeriesType tsType) {
      requireSamples(db, tsType, property.getName(), false, null, constraints.min(), constraints.max(), constraints.regexp(), what);
      return;
    }
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
    // Nothing a stored record holds can violate this declaration (e.g. a plain LINK with no flags): skip the full read
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

  /**
   * The check of a TIMESERIES type, whose samples are not records: they are read from its engine, and the column's value in each is
   * held to the constraint about to be declared by the same rule every write path applies to a new sample
   * ({@link TimeSeriesColumnConstraints}, issue #10238), so a constraint accepted here is one every stored sample satisfies.
   * <p>
   * One shape cannot be judged: the storage writes no value of a STRING column as the empty text, so a stored sample of one never
   * reads back as missing, and MANDATORY / NOTNULL find nothing to refuse there. The write path still holds every new sample to them.
   * MIN and REGEXP judge that empty text here and on the write path alike (issue #10336), so their outcome does not depend on whether
   * the constraint or the sample came first.
   */
  private static void requireSamples(final Database db, final LocalTimeSeriesType type, final String propertyName,
      final boolean required, final String requiredBy, final String min, final String max, final String regexp, final String what) {
    final TimeSeriesEngine engine = type.getEngine();
    final Property property = type.getPolymorphicPropertyIfExists(propertyName);
    if (engine == null || property == null)
      return;

    // the position of the column in a row: the timestamp first, then the other columns in schema order
    final List<ColumnDefinition> columns = type.getTsColumns();
    ColumnDefinition column = null;
    int position = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getName().equals(propertyName)) {
        column = col;
        break;
      }
      if (col.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
        position++;
    }
    if (column == null)
      return;
    final int index = column.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP ? 0 : position + 1;

    final TimeSeriesColumnConstraints.ColumnCheck check = TimeSeriesColumnConstraints.columnCheck(db, column, property.getType(),
        required, requiredBy, min, max, regexp);
    if (check == null)
      return;

    final long regexTimeoutMs = GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(db);
    final boolean stringColumn = column.getDataType() == Type.STRING;
    final Object[] violation = new Object[2];
    try {
      engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
        final String reason = check.check(row[index], regexp != null ? TimeBoundRegex.newDeadline(regexTimeoutMs) : 0L);
        if (reason == null)
          return true;
        violation[0] = row[0];
        // THE EMPTY TEXT IS ALSO WHAT A STRING COLUMN STORES FOR A SAMPLE WRITTEN WITHOUT IT: SAY SO, OR THE REFUSAL OF A VALUE NOBODY
        // WROTE READS AS A MYSTERY
        violation[1] = stringColumn && "".equals(row[index]) ?
            "the column holds the empty text, which is also how it stores a sample written without it, and " + reason :
            reason;
        return false;
      });
    } catch (final IOException e) {
      throw new CommandExecutionException("Cannot set " + what + ": the samples of the TIMESERIES type '" + type.getName()
          + "' could not be read to check it", e);
    }
    if (violation[1] != null)
      // A SAMPLE CANNOT BE UPDATED, BUT IT CAN BE DELETED BY ITS TIME AND ITS TAGS (issues #10336, #10342)
      throw new CommandExecutionException("Cannot set " + what + ": the sample at timestamp " + violation[0] + " of the TIMESERIES "
          + "type '" + type.getName() + "' violates it, " + violation[1] + ". Delete the non-conforming samples first, by their time and "
          + "their tags: DELETE FROM " + type.getName() + " WHERE " + type.getTimestampColumn() + " = " + violation[0] + " [AND <tag> = "
          + "<value> ...], and insert them again if they are to be kept");
  }

  private static String createRefusal(final DocumentType type, final Property property, final Document document,
      final String reason) {
    return "Cannot create property '" + type.getName() + "." + property.getName() + "': record " + document.getIdentity()
        + " violates it, " + reason + ". Fix or delete the non-conforming records first";
  }
}
