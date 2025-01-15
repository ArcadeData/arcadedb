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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Validates documents against constraints defined in the schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DocumentValidator {
  public static void validate(final MutableDocument document) throws ValidationException {
    document.checkForLazyLoadingProperties();
    for (Property entry : document.getType().getPolymorphicProperties())
      validateField(document, entry);
  }

  public static void validateField(final MutableDocument document, final Property p) throws ValidationException {
    if (p.isMandatory() && !document.has(p.getName()))
      throwValidationException(document.getType(), p, "is mandatory, but not found on record: " + document);

    final Object fieldValue = document.get(p.getName());

    if (fieldValue == null) {
      if (p.isNotNull() && document.has(p.getName()))
        // NULLITY
        throwValidationException(document.getType(), p, "cannot be null, record: " + document);
    } else {
      if (p.getRegexp() != null)
        // REGEXP
        if (!(fieldValue.toString()).matches(p.getRegexp()))
          throwValidationException(document.getType(), p,
              "does not match the regular expression '" + p.getRegexp() + "'. Field value is: " + fieldValue + ", record: "
                  + document);

      final Type propertyType = p.getType();

      if (propertyType != null) {
        validateEmbeddedValues(document, p, propertyType, fieldValue);
      }

      if (p.getMin() != null) {
        validateMinValue(document, p, fieldValue);
      }

      if (p.getMax() != null) {
        validateMaxValue(document, p, fieldValue);
      }
    }

    if (p.isReadonly()) {
      if (document.isDirty() && document.getIdentity() != null) {
        final Document originalDocument = ((LocalDatabase) document.getDatabase()).getOriginalDocument(document);
        final Object originalFieldValue = originalDocument.get(p.getName());
        if (!Objects.equals(fieldValue, originalFieldValue))
          throwValidationException(document.getType(), p, "is immutable and cannot be altered. Field value is: " + fieldValue);
      }
    }
  }

  private static void validateMaxValue(MutableDocument document, Property p, Object fieldValue) {
    // CHECK MAX VALUE
    final String max = p.getMax();
    final Type type = p.getType();
    switch (type) {
    case LONG -> {
      final long maxAsLong = Long.parseLong(max);
      if (((Number) fieldValue).longValue() > maxAsLong)
        throwValidationException(document.getType(), p, "value " + fieldValue + " is greater than " + max);
    }
    case INTEGER -> {
      final int maxAsInteger = Integer.parseInt(max);
      if (((Number) fieldValue).intValue() > maxAsInteger)
        throwValidationException(document.getType(), p, "value " + fieldValue + " is greater than " + max);
    }
    case SHORT -> {
      final int maxAsInteger = Integer.parseInt(max);
      if (((Number) fieldValue).shortValue() > maxAsInteger)
        throwValidationException(document.getType(), p, "value " + fieldValue + " is greater than " + max);
    }
    case BYTE -> {
      final int maxAsInteger = Integer.parseInt(max);
      if (((Number) fieldValue).byteValue() > maxAsInteger)
        throwValidationException(document.getType(), p, "value " + fieldValue + " is greater than " + max);
    }
    case FLOAT -> {
      final float maxAsFloat = Float.parseFloat(max);
      if (((Number) fieldValue).floatValue() > maxAsFloat)
        throwValidationException(document.getType(), p, "value " + fieldValue + " is greater than " + max);
    }
    case DOUBLE -> {
      final double maxAsDouble = Double.parseDouble(max);
      if (((Number) fieldValue).floatValue() > maxAsDouble)
        throwValidationException(document.getType(), p, "value " + fieldValue + " is greater than " + max);
    }
    case DECIMAL -> {
      final BigDecimal maxAsDecimal = new BigDecimal(max);
      if (((BigDecimal) fieldValue).compareTo(maxAsDecimal) > 0)
        throwValidationException(document.getType(), p, "value " + fieldValue + " is greater than " + max);
    }
    case STRING -> {
      final int maxAsInteger = Integer.parseInt(max);
      if (fieldValue.toString().length() > maxAsInteger)
        throwValidationException(document.getType(), p, "contains more characters than " + max + " requested");
    }
    case DATE, DATETIME -> {
      final Database database = document.getDatabase();
      final Date maxAsDate = (Date) Type.convert(database, max, Date.class);
      final Date fieldValueAsDate = (Date) Type.convert(database, fieldValue, Date.class);
      if (fieldValueAsDate.compareTo(maxAsDate) > 0)
        throwValidationException(document.getType(), p,
            "contains the date " + fieldValue + " which is after the last acceptable date (" + max + ")");
    }
    case BINARY -> {
      final int maxAsInteger = Integer.parseInt(max);
      if (fieldValue instanceof Binary binary) {
        if (binary.size() > maxAsInteger)
          throwValidationException(document.getType(), p, "contains more bytes than " + max + " requested");
      } else if (((byte[]) fieldValue).length > maxAsInteger)
        throwValidationException(document.getType(), p, "contains more bytes than " + max + " requested");
    }
    case LIST -> {
      final int maxAsInteger = Integer.parseInt(max);
      if (((Collection) fieldValue).size() > maxAsInteger)
        throwValidationException(document.getType(), p, "contains more items than " + max + " requested");
    }
    case MAP -> {
      final int maxAsInteger = Integer.parseInt(max);
      if (((Map) fieldValue).size() > maxAsInteger)
        throwValidationException(document.getType(), p, "contains more items than " + max + " requested");
    }
    default -> throwValidationException(document.getType(), p, "value " + fieldValue + " is greater than " + max);
    }
  }

  private static void validateMinValue(MutableDocument document, Property p, Object fieldValue) {
    // CHECK MIN VALUE
    final String min = p.getMin();
    final ValidationResult result = switch (p.getType()) {
      case LONG -> {
        final long minAsLong = Long.parseLong(min);
        if (((Number) fieldValue).longValue() < minAsLong)
          yield new ValidationResult(true, "value " + fieldValue + " is less than " + min);
        yield new ValidationResult(false, null);
      }
      case INTEGER -> {
        final int minAsInteger = Integer.parseInt(min);
        if (((Number) fieldValue).intValue() < minAsInteger)
          yield new ValidationResult(true, "value " + fieldValue + " is less than " + min);
        yield new ValidationResult(false, null);
      }
      case SHORT -> {
        final int minAsInteger = Integer.parseInt(min);
        if (((Number) fieldValue).shortValue() < minAsInteger)
          yield new ValidationResult(true, "value " + fieldValue + " is less than " + min);
        yield new ValidationResult(false, null);
      }
      case BYTE -> {
        final int minAsInteger = Integer.parseInt(min);
        if (((Number) fieldValue).byteValue() < minAsInteger)
          yield new ValidationResult(true, "value " + fieldValue + " is less than " + min);
        yield new ValidationResult(false, null);
      }
      case FLOAT -> {
        final float minAsFloat = Float.parseFloat(min);
        if (((Number) fieldValue).floatValue() < minAsFloat)
          yield new ValidationResult(true, "value " + fieldValue + " is less than " + min);
        yield new ValidationResult(false, null);
      }
      case DOUBLE -> {
        final double minAsDouble = Double.parseDouble(min);
        if (((Number) fieldValue).floatValue() < minAsDouble)
          yield new ValidationResult(true, "value " + fieldValue + " is less than " + min);
        yield new ValidationResult(false, null);
      }
      case DECIMAL -> {
        final BigDecimal minAsDecimal = new BigDecimal(min);
        if (((BigDecimal) fieldValue).compareTo(minAsDecimal) < 0)
          yield new ValidationResult(true, "value " + fieldValue + " is less than " + min);
        yield new ValidationResult(false, null);
      }
      case STRING -> {
        final int minAsInteger = Integer.parseInt(min);
        if (fieldValue.toString().length() < minAsInteger)
          yield new ValidationResult(true, "contains fewer characters than " + min + " requested");
        yield new ValidationResult(false, null);
      }
      case DATE, DATETIME -> {
        final Database database = document.getDatabase();
        final Date minAsDate = (Date) Type.convert(database, min, Date.class);
        final Date fieldValueAsDate = (Date) Type.convert(database, fieldValue, Date.class);
        if (fieldValueAsDate.compareTo(minAsDate) < 0)
          yield new ValidationResult(true,
              "contains the date " + fieldValue + " which precedes the first acceptable date (" + min + ")");
        yield new ValidationResult(false, null);
      }
      case BINARY -> {
        final int minAsInteger = Integer.parseInt(min);
        if (fieldValue instanceof Binary binary) {
          if (binary.size() < minAsInteger)
            yield new ValidationResult(true, "contains fewer bytes than " + min + " requested");
        } else if (((byte[]) fieldValue).length < minAsInteger)
          yield new ValidationResult(true, "contains fewer bytes than " + min + " requested");
        yield new ValidationResult(false, null);
      }
      case LIST -> {
        final int minAsInteger = Integer.parseInt(min);
        if (((Collection) fieldValue).size() < minAsInteger)
          yield new ValidationResult(true, "contains fewer items than " + min + " requested");
        yield new ValidationResult(false, null);
      }
      case MAP -> {
        final int minAsInteger = Integer.parseInt(min);
        if (((Map) fieldValue).size() < minAsInteger)
          yield new ValidationResult(true, "contains fewer items than " + min + " requested");
        yield new ValidationResult(false, null);
      }
      default -> new ValidationResult(true, "value " + fieldValue + " is less than " + min);
    };

    if (result.hasError)
      throwValidationException(document.getType(), p, result.message);
  }

  private record ValidationResult(boolean hasError, String message) {
  }

  private static void validateEmbeddedValues(MutableDocument document, Property p, Type propertyType, Object fieldValue) {
    final String ofType = p.getOfType();

    // CHECK EMBEDDED VALUES
    switch (propertyType) {
    case LINK: {
      if (fieldValue instanceof EmbeddedDocument)
        throwValidationException(document.getType(), p,
            "has been declared as LINK but an EMBEDDED document is used. Value: " + fieldValue);

      if (ofType != null) {
        final RID rid = ((Identifiable) fieldValue).getIdentity();
        final DocumentType embSchemaType = document.getDatabase().getSchema().getTypeByBucketId(rid.getBucketId());
        if (!embSchemaType.instanceOf(ofType))
          throwValidationException(document.getType(), p,
              "has been declared as LINK of '" + ofType + "' but a link to type '" + embSchemaType + "' is used. Value: "
                  + fieldValue);
      }
    }
    break;

    case EMBEDDED: {
      if (!(fieldValue instanceof EmbeddedDocument))
        throwValidationException(document.getType(), p,
            "has been declared as EMBEDDED but an incompatible type is used. Value: " + fieldValue);

      if (ofType != null) {
        final DocumentType embSchemaType = ((EmbeddedDocument) fieldValue).getType();
        if (!embSchemaType.instanceOf(ofType))
          throwValidationException(document.getType(), p,
              "has been declared as EMBEDDED of '" + ofType + "' but a document of type '" + embSchemaType
                  + "' is used. Value: " + fieldValue);
      }
      if (fieldValue instanceof MutableEmbeddedDocument embeddedDocument)
        embeddedDocument.validate();
    }
    break;

    case LIST: {
      if (!(fieldValue instanceof List))
        throwValidationException(document.getType(), p,
            "has been declared as LIST but an incompatible type is used. Value: " + fieldValue);

      final Type embType = ofType != null ? Type.getTypeByName(ofType) : null;

      for (final Object item : ((List<?>) fieldValue)) {
        if (ofType != null) {
          if (embType != null) {
            if (Type.getTypeByValue(item) != embType)
              throwValidationException(document.getType(), p,
                  "has been declared as LIST of '" + ofType + "' but a value of type '" + Type.getTypeByValue(item)
                      + "' is used. Value: " + fieldValue);
          } else if (item instanceof EmbeddedDocument embeddedDocument) {
            if (!embeddedDocument.getType().instanceOf(ofType))
              throwValidationException(document.getType(), p,
                  "has been declared as LIST of '" + ofType + "' but an embedded document of type '"
                      + embeddedDocument.getType().getName() + "' is used. Value: " + fieldValue);
          } else if (item instanceof Identifiable identifiable) {
            final RID rid = identifiable.getIdentity();
            final DocumentType embSchemaType = document.getDatabase().getSchema().getTypeByBucketId(rid.getBucketId());
            if (!embSchemaType.instanceOf(ofType))
              throwValidationException(document.getType(), p,
                  "has been declared as LIST of '" + ofType + "' but a link to type '" + embSchemaType.getName()
                      + "' is used. Value: "
                      + fieldValue);
          }
        }

        if (item instanceof MutableEmbeddedDocument embeddedDocument)
          embeddedDocument.validate();
      }
    }
    break;

    case MAP: {
      if (!(fieldValue instanceof Map))
        throwValidationException(document.getType(), p,
            "has been declared as MAP but an incompatible type is used. Value: " + fieldValue);

      final Type embType = ofType != null ? Type.getTypeByName(ofType) : null;

      for (final Object item : ((Map<?, ?>) fieldValue).values()) {
        if (ofType != null) {
          if (embType != null) {
            if (Type.getTypeByValue(item) != embType)
              throwValidationException(document.getType(), p,
                  "has been declared as a MAP of <String,'" + ofType + "'> but a value of type '" + Type.getTypeByValue(item)
                      + "' is used. Value: " + fieldValue);
          } else if (item instanceof EmbeddedDocument embeddedDocument) {
            if (!embeddedDocument.getType().instanceOf(ofType))
              throwValidationException(document.getType(), p,
                  "has been declared as a MAP of <String," + ofType + "> but an embedded document of type '"
                      + embeddedDocument.getType().getName() + "' is used. Value: " + fieldValue);
          } else if (item instanceof Identifiable identifiable) {
            final RID rid = identifiable.getIdentity();
            final DocumentType embSchemaType = document.getDatabase().getSchema().getTypeByBucketId(rid.getBucketId());
            if (!embSchemaType.instanceOf(ofType))
              throwValidationException(document.getType(), p,
                  "has been declared as a MAP of <String," + ofType + "> but a link to type '" + embSchemaType.getName()
                      + "' is used. Value: " + fieldValue);
          }
        }

        if (item instanceof MutableEmbeddedDocument embeddedDocument)
          embeddedDocument.validate();
      }
    }
    break;
    }
  }

  private static void throwValidationException(final DocumentType type, final Property p, final String message)
      throws ValidationException {
    throw new ValidationException("The property '" + type.getName() + "." + p.getName() + "' " + message);
  }
}
