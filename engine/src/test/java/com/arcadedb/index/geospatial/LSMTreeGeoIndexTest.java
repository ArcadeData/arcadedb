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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;

import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LSMTreeGeoIndexTest extends TestHelper {

  /**
   * Creates an LSMTreeGeoIndex, registers it with the schema's internal structures,
   * and commits the creation transaction.
   * <p>
   * The underlying LSMTreeIndexMutable constructor requires an active transaction to write its first
   * page. We start one, register the component so the commit machinery can resolve it by file ID
   * (commit2ndPhase) and by name (addFilesToLock), then commit. After this, the index can be used
   * in normal database transactions.
   */
  private LSMTreeGeoIndex createAndRegisterIndex(final String name) throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();

    database.begin();
    final LSMTreeGeoIndex idx = new LSMTreeGeoIndex(
        (DatabaseInternal) database,
        name,
        database.getDatabasePath() + "/" + name,
        ComponentFile.MODE.READ_WRITE,
        LSMTreeIndexAbstract.DEF_PAGE_SIZE,
        LSMTreeIndexAbstract.NULL_STRATEGY.SKIP,
        LSMTreeGeoIndex.DEFAULT_PRECISION
    );

    // Register the paginated component so commit2ndPhase can look it up by file ID
    schema.registerFile(idx.getComponent());

    // Register the index by name so addFilesToLock can resolve it from the schema
    final Field indexMapField = LocalSchema.class.getDeclaredField("indexMap");
    indexMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, IndexInternal> indexMap = (Map<String, IndexInternal>) indexMapField.get(schema);
    indexMap.put(idx.getName(), idx);

    database.commit();
    return idx;
  }

  @Test
  void indexAndQueryPoint() throws Exception {
    final LSMTreeGeoIndex idx = createAndRegisterIndex("test-geo");

    // Index a point in central Italy (lat=45, lon=10)
    final RID rid = new RID(database, 1, 0);
    database.transaction(() -> idx.put(new Object[]{"POINT (10.0 45.0)"}, new RID[]{rid}));

    // Query with a bounding box covering Italy (read-only, outside any transaction)
    final Shape searchShape = GeoUtils.getSpatialContext()
        .getShapeFactory().rect(5.0, 15.0, 40.0, 50.0);

    final IndexCursor cursor = idx.get(new Object[]{searchShape});
    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.next().getIdentity()).isEqualTo(rid);
  }

  @Test
  void pointOutsideQueryReturnsNoResults() throws Exception {
    final LSMTreeGeoIndex idx = createAndRegisterIndex("test-geo2");

    // Pacific coast point, far from Europe
    final RID rid = new RID(database, 1, 0);
    database.transaction(() -> idx.put(new Object[]{"POINT (140.0 35.0)"}, new RID[]{rid}));

    // Search in Europe bounding box
    final Shape searchShape = GeoUtils.getSpatialContext()
        .getShapeFactory().rect(5.0, 15.0, 40.0, 50.0);

    final IndexCursor cursor = idx.get(new Object[]{searchShape});
    assertThat(cursor.hasNext()).isFalse();
  }

  @Test
  void nullWktIsSkippedSilently() throws Exception {
    final LSMTreeGeoIndex idx = createAndRegisterIndex("test-geo3");

    // Should not throw for null WKT — index stays empty (null is skipped)
    final RID rid = new RID(database, 1, 0);
    database.transaction(() -> idx.put(new Object[]{null}, new RID[]{rid}));

    // World bounding box query — nothing was indexed
    final Shape searchShape = GeoUtils.getSpatialContext()
        .getShapeFactory().rect(-180, 180, -90, 90);

    final IndexCursor cursor = idx.get(new Object[]{searchShape});
    assertThat(cursor.hasNext()).isFalse();
  }
}
