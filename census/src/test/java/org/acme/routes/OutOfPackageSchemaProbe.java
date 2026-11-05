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
package org.acme.routes;

import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.List;

/**
 * Never run: compiled only so the census control can show that a walk written outside the {@code com.arcadedb} packages is
 * in range (#11273). The census once read only {@code com/arcadedb} of each module, and this class was invisible to it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class OutOfPackageSchemaProbe {
  private OutOfPackageSchemaProbe() {
  }

  public static List<String> listEveryTypeToTheCaller(final Database database) {
    final List<String> names = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      names.add(type.getName());
    return names;
  }
}
