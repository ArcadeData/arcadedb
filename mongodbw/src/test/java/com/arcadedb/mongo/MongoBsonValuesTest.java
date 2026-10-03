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
package com.arcadedb.mongo;

import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.LegacyUUID;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests of {@link MongoBsonValues} for the shapes a client or another protocol can leave behind (issue #9062).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MongoBsonValuesTest {

  @Test
  void malformedTaggedMapIsReturnedAsIs() {
    final Map<String, Object> bad = new LinkedHashMap<>();
    bad.put(MongoBsonValues.TAG, "timestamp");
    assertThat(MongoBsonValues.toBson(bad)).isSameAs(bad);

    final Map<String, Object> wrongType = new LinkedHashMap<>();
    wrongType.put(MongoBsonValues.TAG, "binData");
    wrongType.put("data", "not bytes");
    assertThat(MongoBsonValues.toBson(wrongType)).isSameAs(wrongType);
  }

  @Test
  void unknownTagIsReturnedAsIs() {
    final Map<String, Object> unknown = new LinkedHashMap<>();
    unknown.put(MongoBsonValues.TAG, "somethingNew");
    assertThat(MongoBsonValues.toBson(unknown)).isSameAs(unknown);
  }

  @Test
  void timestampRoundTrip() {
    final Object stored = MongoBsonValues.toStored(new BsonTimestamp(123456789L));
    assertThat(MongoBsonValues.toBson(stored)).isEqualTo(new BsonTimestamp(123456789L));
  }

  @Test
  void legacyUuidRoundTrip() {
    final LegacyUUID uuid = new LegacyUUID(UUID.randomUUID());
    assertThat(MongoBsonValues.toBson(MongoBsonValues.toStored(uuid))).isEqualTo(uuid);
  }

  @Test
  void unsupportedTypeIsRefused() {
    assertThatThrownBy(() -> MongoBsonValues.toStored(new Object())).isInstanceOf(MongoServerError.class)
        .extracting(e -> ((MongoServerError) e).getCode()).isEqualTo(ErrorCode.BadValue.getValue());
  }

  @Test
  void reservedTagIsRefusedWhateverItsValueType() {
    final Map<String, Object> stringTag = new LinkedHashMap<>();
    stringTag.put(MongoBsonValues.TAG, "timestamp");
    final Map<String, Object> numberTag = new LinkedHashMap<>();
    numberTag.put(MongoBsonValues.TAG, 1);
    final Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("a", stringTag);

    assertThatThrownBy(() -> MongoBsonValues.toStored(stringTag)).isInstanceOf(MongoServerError.class);
    assertThatThrownBy(() -> MongoBsonValues.toStored(numberTag)).isInstanceOf(MongoServerError.class);
    assertThatThrownBy(() -> MongoBsonValues.toStored(nested)).isInstanceOf(MongoServerError.class);
  }
}
