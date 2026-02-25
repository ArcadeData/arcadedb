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

import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GeoIndexMetadataTest {

  @Test
  void defaultPrecision() {
    final GeoIndexMetadata meta = new GeoIndexMetadata("Location", new String[]{"coords"}, 0);
    assertThat(meta.getPrecision()).isEqualTo(GeoIndexMetadata.DEFAULT_PRECISION);
  }

  @Test
  void customPrecisionRoundtrip() {
    final GeoIndexMetadata meta = new GeoIndexMetadata("Location", new String[]{"coords"}, 0);
    meta.setPrecision(7);
    final JSONObject json = new JSONObject();
    meta.toJSON(json);
    assertThat(json.getInt("precision", -1)).isEqualTo(7);

    final GeoIndexMetadata loaded = new GeoIndexMetadata("Location", new String[]{"coords"}, 0);
    loaded.fromJSON(json);
    assertThat(loaded.getPrecision()).isEqualTo(7);
  }
}
