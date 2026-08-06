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
package com.arcadedb.gremlin.io;

import com.arcadedb.database.RID;
import com.arcadedb.gremlin.ArcadeGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the RID coercion helpers. Every arm of the newRID switch and both arms of isRID were
 * previously unexercised (0 of 11 branches).
 */
class ArcadeIoRegistryTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-ioregistry");
    graph.getDatabase().getSchema().createVertexType("Person");
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void aNullYieldsNull() {
    assertThat(ArcadeIoRegistry.newRID(graph.getDatabase(), null)).isNull();
  }

  @Test
  void anExistingRidIsReturnedUnchanged() {
    final RID rid = RID.create(graph.getDatabase(), 1, 42);
    assertThat(ArcadeIoRegistry.newRID(graph.getDatabase(), rid)).isSameAs(rid);
  }

  @Test
  void aStringIsParsedIntoARid() {
    final RID rid = ArcadeIoRegistry.newRID(graph.getDatabase(), "#1:42");
    assertThat(rid.getBucketId()).isEqualTo(1);
    assertThat(rid.getPosition()).isEqualTo(42L);
  }

  @Test
  void aMapWithBucketIdAndPositionIsConvertedToARid() {
    final Map<String, Number> map = new LinkedHashMap<>();
    map.put(ArcadeIoRegistry.BUCKET_ID, 1);
    map.put(ArcadeIoRegistry.BUCKET_POSITION, 42L);
    final RID rid = ArcadeIoRegistry.newRID(graph.getDatabase(), map);
    assertThat(rid.getBucketId()).isEqualTo(1);
    assertThat(rid.getPosition()).isEqualTo(42L);
  }

  @Test
  void anUnsupportedTypeIsRejected() {
    assertThatThrownBy(() -> ArcadeIoRegistry.newRID(graph.getDatabase(), 42))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isRidRecognizesAWellFormedMap() {
    final Map<String, Number> map = new HashMap<>();
    map.put(ArcadeIoRegistry.BUCKET_ID, 1);
    map.put(ArcadeIoRegistry.BUCKET_POSITION, 42L);
    assertThat(ArcadeIoRegistry.isRID(map)).isTrue();
  }

  @Test
  void isRidRejectsANonMap() {
    assertThat(ArcadeIoRegistry.isRID("#1:42")).isFalse();
  }

  @Test
  void isRidRejectsAMapMissingTheBucketPosition() {
    final Map<String, Number> map = new HashMap<>();
    map.put(ArcadeIoRegistry.BUCKET_ID, 1);
    assertThat(ArcadeIoRegistry.isRID(map)).isFalse();
  }

  @Test
  void isRidRejectsAMapMissingTheBucketId() {
    final Map<String, Number> map = new HashMap<>();
    map.put(ArcadeIoRegistry.BUCKET_POSITION, 42L);
    assertThat(ArcadeIoRegistry.isRID(map)).isFalse();
  }

  @Test
  void theSharedInstanceIsNotNullAndHasNoDatabase() {
    assertThat(ArcadeIoRegistry.instance()).isNotNull();
    assertThat(ArcadeIoRegistry.instance().getDatabase()).isNull();
  }
}
