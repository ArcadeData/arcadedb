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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpNodeReaderTest {

  private static JSONArray rows(final long... keys) {
    final JSONArray rows = new JSONArray();
    for (final long key : keys)
      rows.put(new JSONObject().put("id", key).put("e", 0));
    return rows;
  }

  @Test
  void exactMultipleOfPageSizeTerminates() throws Exception {
    final Ledger ledger = new Ledger(1);
    for (int i = 0; i < 3; i++)
      ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    final List<Long> requested = new ArrayList<>();
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    HttpNodeReader.scanPages(last -> {
      requested.add(last);
      return last < 0 ? rows(Ledger.key(0, 0), Ledger.key(0, 1), Ledger.key(0, 2)) : rows();
    }, snapshot, 3);
    assertThat(requested).containsExactly(-1L, Ledger.key(0, 2));
    assertThat(snapshot.rows()).isEqualTo(3);
    assertThat(snapshot.duplicates()).isEmpty();
  }

  @Test
  void partialPageStopsAfterOneRequest() throws Exception {
    final Ledger ledger = new Ledger(1);
    ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    final List<Long> requested = new ArrayList<>();
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    HttpNodeReader.scanPages(last -> {
      requested.add(last);
      return rows(Ledger.key(0, 0));
    }, snapshot, 3);
    assertThat(requested).containsExactly(-1L);
    assertThat(snapshot.present(0, 0)).isTrue();
  }

  @Test
  void edgeCountIsPassedThrough() throws Exception {
    final Ledger ledger = new Ledger(1);
    ledger.record(ledger.reserve(0, true), Ledger.ACKED);
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    HttpNodeReader.scanPages(last -> new JSONArray().put(new JSONObject().put("id", Ledger.key(0, 0)).put("e", 1)), snapshot, 3);
    assertThat(snapshot.hasEdge(0, 0)).isTrue();
  }

  @Test
  void serverErrorIsReportedWithItsStatus() {
    assertThatThrownBy(() -> HttpNodeReader.checkStatus(1, 500, "boom"))
        .isInstanceOfSatisfying(HttpNodeReader.ServerErrorException.class, e -> assertThat(e.status()).isEqualTo(500))
        .hasMessageContaining("node 1").hasMessageContaining("HTTP 500").hasMessageContaining("boom");
    assertThatThrownBy(() -> HttpNodeReader.checkStatus(1, 503, "")).isInstanceOf(HttpNodeReader.ServerErrorException.class);
  }

  @Test
  void clientErrorIsAPlainIOException() throws Exception {
    assertThatThrownBy(() -> HttpNodeReader.checkStatus(2, 401, "denied")).isInstanceOf(IOException.class)
        .isNotInstanceOf(HttpNodeReader.ServerErrorException.class).hasMessageContaining("HTTP 401");
    HttpNodeReader.checkStatus(2, 200, "");
  }
}
