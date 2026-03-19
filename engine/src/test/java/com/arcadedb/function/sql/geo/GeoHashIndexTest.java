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
package com.arcadedb.function.sql.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.io.GeohashUtils;

import static org.assertj.core.api.Assertions.assertThat;

class GeoHashIndexTest {

  @Test
  void geoManualIndexPoints() throws Exception {
    final int TOTAL = 1_000;

    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {

      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Restaurant");
        type.createProperty("coords", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

        for (int i = 0; i < TOTAL; i++) {
          final MutableDocument doc = db.newDocument("Restaurant");
          doc.set("lat", 10 + (0.01D * i));
          doc.set("long", 10 + (0.01D * i));
          doc.set("coords", GeohashUtils.encodeLatLon(doc.getDouble("lat"), doc.getDouble("long")));
          doc.save();
        }

        final String[] area = new String[] { GeohashUtils.encodeLatLon(10.5, 10.5), GeohashUtils.encodeLatLon(10.55, 10.55) };

        ResultSet result = db.query("sql", "select from Restaurant where coords >= ? and coords <= ?", area[0], area[1]);

        assertThat(result.hasNext()).isTrue();
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          assertThat(record.getDouble("lat")).isGreaterThanOrEqualTo(10.5);
          assertThat(record.getDouble("long")).isLessThanOrEqualTo(10.55);
          ++returned;
        }

        assertThat(returned).isEqualTo(6);
      });
    });
  }

  @Test
  void geoManualIndexBoundingBoxes() throws Exception {
    final int TOTAL = 1_000;

    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {

      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Restaurant");
        type.createProperty("bboxTL", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
        type.createProperty("bboxBR", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

        for (int i = 0; i < TOTAL; i++) {
          final MutableDocument doc = db.newDocument("Restaurant");
          doc.set("x1", 10D + (0.0001D * i));
          doc.set("y1", 10D + (0.0001D * i));
          doc.set("x2", 10D + (0.001D * i));
          doc.set("y2", 10D + (0.001D * i));
          doc.set("bboxTL", GeohashUtils.encodeLatLon(doc.getDouble("x1"), doc.getDouble("y1")));
          doc.set("bboxBR", GeohashUtils.encodeLatLon(doc.getDouble("x2"), doc.getDouble("y2")));
          doc.save();
        }

        for (Index idx : type.getAllIndexes(false))
          assertThat(idx.countEntries()).isEqualTo(TOTAL);

        final String[] area = new String[] {
            GeohashUtils.encodeLatLon(10.0001D, 10.0001D),
            GeohashUtils.encodeLatLon(10.020D, 10.020D) };

        ResultSet result = db.query("sql", "select from Restaurant where bboxTL >= ? and bboxBR <= ?", area[0], area[1]);

        assertThat(result.hasNext()).isTrue();
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          assertThat(record.getDouble("x1")).isGreaterThanOrEqualTo(10.0001D).withFailMessage("x1: " + record.getDouble("x1"));
          assertThat(record.getDouble("y1")).isGreaterThanOrEqualTo(10.0001D).withFailMessage("y1: " + record.getDouble("y1"));
          assertThat(record.getDouble("x2")).isLessThanOrEqualTo(10.020D).withFailMessage("x2: " + record.getDouble("x2"));
          assertThat(record.getDouble("y2")).isLessThanOrEqualTo(10.020D).withFailMessage("y2: " + record.getDouble("y2"));
          ++returned;
        }

        assertThat(returned).isEqualTo(20);
      });
    });
  }
}
