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
package com.arcadedb.census;

import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Never run: compiled only so {@link SchemaPresentationReadableWalkCensusTest#theCensusSeesEveryShapeOfTheWalk} can read
 * the shapes of the walk the source-text census missed (issue #11197) and the two it must not report.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CensusProbe {
  private CensusProbe() {
  }

  /** The schema hoisted into a local whose name is not {@code schema}, the walk on the next line. */
  static List<String> hoisted(final Database database) {
    final Schema s = database.getSchema();
    final List<String> names = new ArrayList<>();
    for (final DocumentType type : s.getTypes())
      names.add(type.getName());
    return names;
  }

  static Supplier<Collection<? extends DocumentType>> methodReference(final Schema schemaOfTheCaller) {
    return schemaOfTheCaller::getTypes;
  }

  static int onTheImplementation(final LocalSchema local) {
    return local.getTypes().size();
  }

  static Runnable inALambda(final Database database) {
    return () -> database.getSchema().getTypes().forEach(type -> type.getName());
  }

  static int readable(final Database database) {
    return SecurityHelper.readableTypes(database).size();
  }

  static int notASchema(final Registry registry) {
    return registry.getTypes().size();
  }

  /** Another {@code getTypes()}, on a class that is no schema. */
  interface Registry {
    Collection<? extends DocumentType> getTypes();
  }
}
