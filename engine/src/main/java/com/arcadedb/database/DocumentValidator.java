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

import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import java.math.*;
import java.util.*;

/**
 * Validates documents against constraints defined in the schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DocumentValidator {

  // TODO move ACCM validation here?

  public static void validateSpecificProperties(final MutableDocument document, final List<Property> properties) throws ValidationException {
    document.checkForLazyLoadingProperties();
    for (Property entry : properties)
      validateField(document, entry);
  }

  public static void validate(final MutableDocument document) throws ValidationException {
    document.checkForLazyLoadingProperties();
    for (Property entry : document.getType().getProperties())
      validateField(document, entry);
  }

  public static void validateField(final MutableDocument document, final Property p) throws ValidationException {
    if (p.isMandatory() && !document.has(p.getName()))
      throwValidationException(p, "is mandatory, but not found on record: " + document);

    final Object fieldValue = document.get(p.getName());

    if (fieldValue == null) {
      if (p.isNotNull() && document.has(p.getName()))
        // NULLITY
        throwValidationException(p, "cannot be null, record: " + document);
    } else {
      if (p.getRegexp() != null)
        // REGEXP
        if (!(fieldValue.toString()).matches(p.getRegexp()))
          throwValidationException(p, "does not match the regular expression '" + p.getRegexp() + "'. Field value is: " + fieldValue + ", record: " + document);

      final Type propertyType = p.getType();

      if (propertyType != null) {
        // CHECK EMBEDDED VALUES
        switch (propertyType) {
        case EMBEDDED:
          if (fieldValue instanceof MutableEmbeddedDocument)
            ((MutableEmbeddedDocument) fieldValue).validate();
          break;
        case LIST:
          if (!(fieldValue instanceof List))
            throwValidationException(p, "has been declared as LIST but an incompatible type is used. Value: " + fieldValue);

          for (final Object item : ((List<?>) fieldValue)) {
            if (item instanceof MutableEmbeddedDocument)
              ((MutableEmbeddedDocument) item).validate();
          }
          break;

        case MAP:
          if (!(fieldValue instanceof Map))
            throwValidationException(p, "has been declared as MAP but an incompatible type is used. Value: " + fieldValue);

          for (final Object item : ((Map<?, ?>) fieldValue).values()) {
            if (item instanceof MutableEmbeddedDocument)
              ((MutableEmbeddedDocument) item).validate();
          }
          break;
        }
      }

      if (p.getMin() != null) {
        // CHECK MIN VALUE
        final String min = p.getMin();
        switch (p.getType()) {

        case LONG: {
          final long minAsLong = Long.parseLong(min);
          if (((Number) fieldValue).longValue() < minAsLong)
            throwValidationException(p, "value " + fieldValue + " is less than " + min);
          break;
        }

        case INTEGER: {
          final int minAsInteger = Integer.parseInt(min);
          if (((Number) fieldValue).intValue() < minAsInteger)
            throwValidationException(p, "value " + fieldValue + " is less than " + min);
          break;
        }

        case SHORT: {
          final int minAsInteger = Integer.parseInt(min);
          if (((Number) fieldValue).shortValue() < minAsInteger)
            throwValidationException(p, "value " + fieldValue + " is less than " + min);
          break;
        }

        case BYTE: {
          final int minAsInteger = Integer.parseInt(min);
          if (((Number) fieldValue).byteValue() < minAsInteger)
            throwValidationException(p, "value " + fieldValue + " is less than " + min);
          break;
        }

        case FLOAT: {
          final float minAsFloat = Float.parseFloat(min);
          if (((Number) fieldValue).floatValue() < minAsFloat)
            throwValidationException(p, "value " + fieldValue + " is less than " + min);
          break;
        }

        case DOUBLE: {
          final double minAsDouble = Double.parseDouble(min);
          if (((Number) fieldValue).floatValue() < minAsDouble)
            throwValidationException(p, "value " + fieldValue + " is less than " + min);
          break;
        }

        case DECIMAL: {
          final BigDecimal minAsDecimal = new BigDecimal(min);
          if (((BigDecimal) fieldValue).compareTo(minAsDecimal) < 0)
            throwValidationException(p, "value " + fieldValue + " is less than " + min);
          break;
        }

        case STRING: {
          final int minAsInteger = Integer.parseInt(min);
          if (fieldValue.toString().length() < minAsInteger)
            throwValidationException(p, "contains fewer characters than " + min + " requested");
          break;
        }

        case DATE:
        case DATETIME: {
          final Database database = document.getDatabase();
          final Date minAsDate = (Date) Type.convert(database, min, Date.class);
          final Date fieldValueAsDate = (Date) Type.convert(database, fieldValue, Date.class);

          if (fieldValueAsDate.compareTo(minAsDate) < 0)
            throwValidationException(p, "contains the date " + fieldValue + " which precedes the first acceptable date (" + min + ")");
          break;
        }

        case BINARY: {
          final int minAsInteger = Integer.parseInt(min);
          if (fieldValue instanceof Binary) {
            if (((Binary) fieldValue).size() < minAsInteger)
              throwValidationException(p, "contains fewer bytes than " + min + " requested");
          } else if (((byte[]) fieldValue).length < minAsInteger)
            throwValidationException(p, "contains fewer bytes than " + min + " requested");
          break;
        }

        case LIST: {
          final int minAsInteger = Integer.parseInt(min);
          if (((Collection) fieldValue).size() < minAsInteger)
            throwValidationException(p, "contains fewer items than " + min + " requested");
          break;
        }

        case MAP: {
          final int minAsInteger = Integer.parseInt(min);
          if (((Map) fieldValue).size() < minAsInteger)
            throwValidationException(p, "contains fewer items than " + min + " requested");
          break;
        }

        default:
          throwValidationException(p, "value " + fieldValue + " is less than " + min);
        }
      }

      if (p.getMax() != null) {
        // CHECK MAX VALUE
        final String max = p.getMax();

        switch (p.getType()) {
        case LONG: {
          final long maxAsLong = Long.parseLong(max);
          if (((Number) fieldValue).longValue() > maxAsLong)
            throwValidationException(p, "value " + fieldValue + " is greater than " + max);
          break;
        }

        case INTEGER: {
          final int maxAsInteger = Integer.parseInt(max);
          if (((Number) fieldValue).intValue() > maxAsInteger)
            throwValidationException(p, "value " + fieldValue + " is greater than " + max);
          break;
        }

        case SHORT: {
          final int maxAsInteger = Integer.parseInt(max);
          if (((Number) fieldValue).shortValue() > maxAsInteger)
            throwValidationException(p, "value " + fieldValue + " is greater than " + max);
          break;
        }

        case BYTE: {
          final int maxAsInteger = Integer.parseInt(max);
          if (((Number) fieldValue).byteValue() > maxAsInteger)
            throwValidationException(p, "value " + fieldValue + " is greater than " + max);
          break;
        }

        case FLOAT: {
          final float maxAsFloat = Float.parseFloat(max);
          if (((Number) fieldValue).floatValue() > maxAsFloat)
            throwValidationException(p, "value " + fieldValue + " is greater than " + max);
          break;
        }

        case DOUBLE: {
          final double maxAsDouble = Double.parseDouble(max);
          if (((Number) fieldValue).floatValue() > maxAsDouble)
            throwValidationException(p, "value " + fieldValue + " is greater than " + max);
          break;
        }

        case DECIMAL: {
          final BigDecimal maxAsDecimal = new BigDecimal(max);
          if (((BigDecimal) fieldValue).compareTo(maxAsDecimal) > 0)
            throwValidationException(p, "value " + fieldValue + " is greater than " + max);
          break;
        }

        case STRING: {
          final int maxAsInteger = Integer.parseInt(max);
          if (fieldValue.toString().length() > maxAsInteger)
            throwValidationException(p, "contains more characters than " + max + " requested");
          break;
        }

        case DATE:
        case DATETIME: {
          final Database database = document.getDatabase();
          final Date maxAsDate = (Date) Type.convert(database, max, Date.class);
          final Date fieldValueAsDate = (Date) Type.convert(database, fieldValue, Date.class);

          if (fieldValueAsDate.compareTo(maxAsDate) > 0)
            throwValidationException(p, "contains the date " + fieldValue + " which is after the last acceptable date (" + max + ")");
          break;
        }

        case BINARY: {
          final int maxAsInteger = Integer.parseInt(max);
          if (fieldValue instanceof Binary) {
            if (((Binary) fieldValue).size() > maxAsInteger)
              throwValidationException(p, "contains more bytes than " + max + " requested");
          } else if (((byte[]) fieldValue).length > maxAsInteger)
            throwValidationException(p, "contains more bytes than " + max + " requested");
          break;
        }

        case LIST: {
          final int maxAsInteger = Integer.parseInt(max);
          if (((Collection) fieldValue).size() > maxAsInteger)
            throwValidationException(p, "contains more items than " + max + " requested");
          break;
        }

        case MAP: {
          final int maxAsInteger = Integer.parseInt(max);
          if (((Map) fieldValue).size() > maxAsInteger)
            throwValidationException(p, "contains more items than " + max + " requested");
          break;
        }

        default:
          throwValidationException(p, "value " + fieldValue + " is greater than " + max);
        }
      }
    }

    if (p.isReadonly()) {
      if (document.isDirty() && document.getIdentity() != null) {
        final Document originalDocument = ((EmbeddedDatabase) document.getDatabase()).getOriginalDocument(document);
        final Object originalFieldValue = originalDocument.get(p.getName());
        if (!Objects.equals(fieldValue, originalFieldValue))
          throwValidationException(p, "is immutable and cannot be altered. Field value is: " + fieldValue);
      }
    }
  }

  private static void throwValidationException(final Property p, final String message) throws ValidationException {
    throw new ValidationException("The property '" + p.getName() + "' " + message);
  }
}
