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
package com.arcadedb.engine.timeseries;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A tag is stored by its text form on every protocol, so a value whose text form means nothing must be refused
 * rather than stored (claude-review on PR #7323, issue #7305).
 * <p>
 * The sharp case is {@code byte[]}: it has no {@code toString()} override, so {@code String.valueOf} yields an
 * object identity like {@code [B@6bc7c054} - a DIFFERENT tag on every run, which silently splits one series
 * into many and matches no filter. The proto types tags as {@code map<string, GrpcValue>}, so nothing at the
 * wire boundary stops one arriving.
 */
class TimeSeriesGatewayTagValueTest {

  @Test
  void aByteArrayTagIsRefusedRatherThanStoredAsAnObjectIdentity() {
    assertThatThrownBy(() -> TimeSeriesGateway.requireStorableTagValue("host", new byte[] { 1, 2, 3 }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host")
        .hasMessageContaining("byte[]");
  }

  @Test
  void anyArrayIsRefused() {
    // Not only byte[]: every array type inherits Object.toString().
    assertThatThrownBy(() -> TimeSeriesGateway.requireStorableTagValue("host", new int[] { 1 }))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> TimeSeriesGateway.requireStorableTagValue("host", new String[] { "a" }))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void collectionsAndMapsAreRefused() {
    // Their text form is stable, unlike an array's, but it is not a tag value anyone means - and a tag column
    // would coerce it to something arbitrary.
    assertThatThrownBy(() -> TimeSeriesGateway.requireStorableTagValue("host", List.of("a", "b")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> TimeSeriesGateway.requireStorableTagValue("host", Map.of("a", "b")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theScalarsATagIsActuallyMadeOfPassThroughUnchanged() {
    // The check must not become a whitelist that rejects ordinary tags: anything with a meaningful toString()
    // is storable, because that text is exactly what the column coerces.
    for (final Object value : new Object[] { "us-east", 42, 42L, 4.2, true, 'c', java.math.BigDecimal.ONE })
      assertThat(TimeSeriesGateway.requireStorableTagValue("host", value)).isSameAs(value);
  }

  @Test
  void aNullTagValueIsNotRefusedHere() {
    // Null means "no tag on this sample" and every writer drops it before this point; refusing it here would
    // turn an omission into an error.
    assertThat(TimeSeriesGateway.requireStorableTagValue("host", null)).isNull();
  }

  @Test
  void theLineProtocolWriterRefusesItToo() {
    // The HTTP client builds its body through this writer, so the same value must be refused on that path and
    // not silently written as an object identity.
    assertThatThrownBy(() -> LineProtocolWriter.appendLine(new StringBuilder(), "m",
        Map.of("host", new byte[] { 1 }), Map.of("v", 1.0), 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");

    assertThatCode(() -> LineProtocolWriter.appendLine(new StringBuilder(), "m",
        Map.of("host", "web-1"), Map.of("v", 1.0), 1L)).doesNotThrowAnyException();
  }
}
