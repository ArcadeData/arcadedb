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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedViewTest extends TestHelper {

  @Test
  void jsonRoundTrip() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, "SalesSummary",
        "SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer",
        "SalesSummary", List.of("Order"),
        MaterializedViewRefreshMode.MANUAL, false, 0);

    final JSONObject json = view.toJSON();
    final MaterializedViewImpl restored = MaterializedViewImpl.fromJSON(database, json);

    assertThat(restored.getName()).isEqualTo("SalesSummary");
    assertThat(restored.getQuery()).isEqualTo("SELECT customer, SUM(amount) AS total FROM Order GROUP BY customer");
    assertThat(restored.getSourceTypeNames()).containsExactly("Order");
    assertThat(restored.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.MANUAL);
    assertThat(restored.getStatus()).isEqualTo("VALID");
    assertThat(restored.isSimpleQuery()).isFalse();
  }

  @Test
  void statusTransitions() {
    final MaterializedViewImpl view = new MaterializedViewImpl(
        database, "TestView",
        "SELECT name FROM User WHERE active = true",
        "TestView", List.of("User"),
        MaterializedViewRefreshMode.MANUAL, true, 0);

    assertThat(view.getStatus()).isEqualTo("VALID");

    view.setStatus("STALE");
    assertThat(view.getStatus()).isEqualTo("STALE");

    view.setStatus("BUILDING");
    assertThat(view.getStatus()).isEqualTo("BUILDING");

    view.setStatus("ERROR");
    assertThat(view.getStatus()).isEqualTo("ERROR");

    view.setStatus("VALID");
    view.updateLastRefreshTime();
    assertThat(view.getLastRefreshTime()).isGreaterThan(0);
  }
}
