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
package com.arcadedb.query.opencypher.procedures.meta;

import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for meta/schema procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractMetaProcedure implements CypherProcedure {

  protected Map<String, Object> typeToMap(final DocumentType type) {
    final Map<String, Object> result = new HashMap<>();
    result.put("name", type.getName());
    result.put("type", type.getClass().getSimpleName().replace("Type", "").toUpperCase());

    final List<Map<String, Object>> properties = new ArrayList<>();
    for (final String propName : type.getPropertyNames()) {
      final Property property = type.getProperty(propName);
      final Map<String, Object> propMap = new HashMap<>();
      propMap.put("name", propName);
      propMap.put("type", property.getType().name());
      propMap.put("mandatory", property.isMandatory());
      propMap.put("readOnly", property.isReadonly());
      if (property.getDefaultValue() != null) {
        propMap.put("defaultValue", property.getDefaultValue());
      }
      properties.add(propMap);
    }
    result.put("properties", properties);

    return result;
  }

  protected String mapPropertyType(final com.arcadedb.schema.Type type) {
    return switch (type) {
      case STRING -> "String";
      case INTEGER -> "Integer";
      case LONG -> "Long";
      case SHORT -> "Short";
      case BYTE -> "Byte";
      case FLOAT -> "Float";
      case DOUBLE -> "Double";
      case BOOLEAN -> "Boolean";
      case DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND -> "DateTime";
      case DATE -> "Date";
      case BINARY -> "ByteArray";
      case LIST -> "List";
      case MAP -> "Map";
      case EMBEDDED -> "Map";
      case LINK -> "Relationship";
      case DECIMAL -> "BigDecimal";
      case ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS, ARRAY_OF_LONGS, ARRAY_OF_FLOATS, ARRAY_OF_DOUBLES -> "NumberArray";
      default -> type.name();
    };
  }
}
