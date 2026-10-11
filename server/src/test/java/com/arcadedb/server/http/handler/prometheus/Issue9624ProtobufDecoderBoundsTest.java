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
package com.arcadedb.server.http.handler.prometheus;

import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9624: the decoder-level halves of the fix - a truncated skipped fixed64 fails instead of decoding to an empty
 * request, the {@code proto=} parameter of a Content-Type is read, and a 1.0 metadata-only request is told apart from
 * a body carrying no 1.0 content at all.
 */
class Issue9624ProtobufDecoderBoundsTest {

  @Test
  void aTruncatedSkippedFixed64Fails() {
    // field 1, fixed64, 1 byte instead of 8: used to be skipped past the end and decode to an empty request
    assertThatThrownBy(() -> WriteRequest.decode(HexFormat.of().parseHex("0902"))).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("fixed64");
  }

  @Test
  void aBodyWithOnlyUnknownFieldsHasNoRemoteWrite1Content() {
    // field 4 (a remote write 2.0 symbol), field 5 (a 2.0 series): legal protobuf, but nothing a 1.0 WriteRequest names
    final ProtobufEncoder request = new ProtobufEncoder();
    request.writeString(4, "__name__");
    request.writeSubMessage(5, new byte[] { 0x08, 0x01 });
    final WriteRequest decoded = WriteRequest.decode(request.toByteArray());
    assertThat(decoded.getTimeSeries()).isEmpty();
    assertThat(decoded.hasRecognizedContent()).isFalse();
  }

  @Test
  void metadataCountsAsRemoteWrite1Content() {
    final ProtobufEncoder metadata = new ProtobufEncoder();
    metadata.writeString(2, "up");
    final ProtobufEncoder request = new ProtobufEncoder();
    request.writeSubMessage(3, metadata.toByteArray());

    final WriteRequest decoded = WriteRequest.decode(request.toByteArray());
    assertThat(decoded.getTimeSeries()).isEmpty();
    assertThat(decoded.hasRecognizedContent()).isTrue();
  }

  @Test
  void protoParameterIsReadFromTheContentType() {
    assertThat(PrometheusTypes.protoMessage(null)).isNull();
    assertThat(PrometheusTypes.protoMessage("application/x-protobuf")).isNull();
    assertThat(PrometheusTypes.protoMessage("application/x-protobuf;proto=io.prometheus.write.v2.Request"))
        .isEqualTo("io.prometheus.write.v2.Request");
    assertThat(PrometheusTypes.protoMessage("application/x-protobuf; charset=utf-8; PROTO=\"prometheus.WriteRequest\" "))
        .isEqualTo("prometheus.WriteRequest");
    assertThat(PrometheusTypes.protoMessage("application/x-protobuf;proto=")).isEmpty();
  }
}
