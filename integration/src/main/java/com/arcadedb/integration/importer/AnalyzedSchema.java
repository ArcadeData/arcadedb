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
package com.arcadedb.integration.importer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class AnalyzedSchema {
  private final Map<String, AnalyzedEntity> entities = new LinkedHashMap<>();
  private final long                        maxValueSampling;

  public AnalyzedSchema(final long maxValueSampling) {
    this.maxValueSampling = maxValueSampling;
  }

  public AnalyzedEntity getOrCreateEntity(final String entityName, final AnalyzedEntity.ENTITY_TYPE entityType) {
    AnalyzedEntity entity = entities.get(entityName);
    if (entity == null) {
      entity = new AnalyzedEntity(entityName, entityType, maxValueSampling);
      entities.put(entityName, entity);
    }
    return entity;
  }

  public void endParsing() {
    for (AnalyzedEntity entity : entities.values())
      for (AnalyzedProperty property : entity.getProperties())
        property.endParsing();
  }

  public Collection<AnalyzedEntity> getEntities() {
    return entities.values();
  }

  public AnalyzedEntity getEntity(final String name) {
    return entities.get(name);
  }
}
