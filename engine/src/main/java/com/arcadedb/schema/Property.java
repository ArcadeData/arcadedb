package com.arcadedb.schema;/*
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

import com.arcadedb.index.Index;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.*;

/**
 * Schema Property.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface Property {

  String RID_PROPERTY            = "@rid";
  String TYPE_PROPERTY           = "@type";
  String IN_PROPERTY             = "@in";
  String OUT_PROPERTY            = "@out";
  String CAT_PROPERTY            = "@cat";
  String PROPERTY_TYPES_PROPERTY = "@props";

  Set<String> METADATA_PROPERTIES = Set.of(RID_PROPERTY, TYPE_PROPERTY, IN_PROPERTY, OUT_PROPERTY, CAT_PROPERTY,
      PROPERTY_TYPES_PROPERTY);

  Index createIndex(Schema.INDEX_TYPE type, boolean unique);

  Index getOrCreateIndex(Schema.INDEX_TYPE type, boolean unique);

  String getName();

  Type getType();

  int getId();

  Object getDefaultValue();

  Property setDefaultValue(Object defaultValue);

  String getOfType();

  Property setOfType(String ofType);

  Property setReadonly(boolean readonly);

  boolean isReadonly();

  Property setMandatory(boolean mandatory);

  boolean isMandatory();

  Property setNotNull(boolean notNull);

  boolean isNotNull();

  Property setHidden(boolean hidden);

  boolean isHidden();

  Property setMax(String max);

  String getMax();

  Property setMin(String min);

  String getMin();

  Property setRegexp(String regexp);

  String getRegexp();

  Set<String> getCustomKeys();

  Object getCustomValue(String key);

  JSONObject toJSON();

  Object setCustomValue(String key, Object value);
}
