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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Rectangle;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The FRONTIER layout (issue #5478) resolves a query with a GeoHash prefix RANGE SCAN instead of one exact lookup per
 * covering cell, so it reads the index through {@code LSMTreeIndexCursor} - the merge across the mutable pages AND every
 * compacted series - where the old layout only ever did point lookups.
 * <p>
 * This pins that path: enough points to force compaction, deletions to leave tombstones behind in older series, then
 * every window query checked against a brute-force scan. The index is allowed to return a superset (it approximates the
 * shape with cells) but never a subset, and never a RID that no longer exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class LSMTreeGeoIndexCompactedRangeTest extends TestHelper {
  private static final int RECORDS = 60_000;

  @Test
  void rangeScanAgreesWithFullScanAcrossCompactedSeries() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Place", 1).createProperty("location", Type.STRING);
      database.getSchema().getType("Place").createProperty("seq", Type.INTEGER);
    });
    database.command("sql", "CREATE INDEX ON Place (location) GEOSPATIAL");

    final Random rnd = new Random(7);
    for (int i = 0; i < RECORDS; i += 1_000) {
      final int from = i;
      database.transaction(() -> {
        for (int j = from; j < from + 1_000; j++) {
          final MutableDocument doc = database.newDocument("Place");
          doc.set("seq", j);
          doc.set("location", "POINT (" + (8.0 + rnd.nextDouble() * 5.0) + " " + (54.5 + rnd.nextDouble() * 3.0) + ")");
          doc.save();
        }
      });
    }

    // Deletions in a separate transaction so the LSM writes real tombstones (a remove+add inside one transaction is
    // collapsed by TransactionIndexContext and never reaches the index as a tombstone).
    database.transaction(() -> database.command("sql", "DELETE FROM Place WHERE seq % 7 = 0"));

    final Index index = database.getSchema().getIndexByName("Place[location]");
    for (final IndexInternal sub : ((TypeIndex) index).getIndexesOnBuckets()) {
      sub.scheduleCompaction();
      sub.compact();
    }
    database.async().waitCompletion();

    for (int i = 0; i < 20; i++) {
      final double lon = 8.0 + rnd.nextDouble() * 4.5;
      final double lat = 54.5 + rnd.nextDouble() * 2.5;
      final double size = 0.01 + rnd.nextDouble() * 0.5;
      final Rectangle window = GeoUtils.getSpatialContext().getShapeFactory()
          .rect(lon, lon + size, lat, lat + size);

      final Set<RID> fromIndex = new HashSet<>();
      for (final IndexCursor cursor = index.get(new Object[] { window }); cursor.hasNext(); ) {
        final Identifiable next = cursor.next();
        if (next != null)
          fromIndex.add(next.getIdentity());
      }

      // Ground truth: every live record whose point is inside the window
      final Set<RID> fromScan = new HashSet<>();
      database.transaction(() -> database.scanType("Place", true, record -> {
        // get(), not getString(): the serializer hands a WKT-looking STRING back as a spatial4j Shape
        if (window.relate(GeoUtils.parseGeometry(record.asDocument().get("location"))).intersects())
          fromScan.add(record.getIdentity());
        return true;
      }));

      // The index approximates the shape with GeoHash cells: a superset is expected, a miss is a bug
      assertThat(fromIndex).as("window %s", window).containsAll(fromScan);
      // Every RID it returns must still be a live record
      for (final RID rid : fromIndex)
        assertThat(database.lookupByRID(rid, false)).isNotNull();
    }
  }
}
