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
package com.arcadedb.index.geospatial;

import com.arcadedb.TestHelper;
import com.arcadedb.index.Index;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LSMTreeGeoIndexSchemaTest extends TestHelper {

  @Test
  void createGeospatialIndexViaSql() {
    database.command("sql", "CREATE DOCUMENT TYPE Location");
    database.command("sql", "CREATE PROPERTY Location.coords STRING");
    database.command("sql", "CREATE INDEX ON Location (coords) GEOSPATIAL");

    database.transaction(() -> database.command("sql", "INSERT INTO Location SET coords = 'POINT (12.5 41.9)'"));

    final Index index = database.getSchema().getIndexByName("Location[coords]");
    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
  }
}
