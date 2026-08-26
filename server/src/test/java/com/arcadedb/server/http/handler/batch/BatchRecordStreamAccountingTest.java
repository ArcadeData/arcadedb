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
package com.arcadedb.server.http.handler.batch;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every line a batch stream consumes either becomes a record or is one it declares skipped. That equality is what
 * lets the endpoint - and the client - prove that a load did not quietly lose records, which is the question
 * issue #5618 had no way to answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BatchRecordStreamAccountingTest {

  @Test
  void jsonlCountsEveryLineItReads() throws Exception {
    final String payload = """
        {"@type":"vertex","@class":"V1","@id":"a"}

        {"@type":"vertex","@class":"V1","@id":"b"}
        \s
        {"@type":"edge","@class":"E1","@from":"a","@to":"b"}
        """;

    final int records = drain(new JsonlBatchRecordStream(stream(payload)), 3);
    final JsonlBatchRecordStream counted = new JsonlBatchRecordStream(stream(payload));
    drain(counted, records);

    assertThat(counted.getLinesRead()).isEqualTo(5);
    assertThat(counted.getLinesSkipped()).isEqualTo(2);
    assertThat(counted.getLinesRead() - counted.getLinesSkipped()).isEqualTo(records);
  }

  /**
   * CSV skips more than blank lines: the header of each section and the {@code ---} separator carry no record
   * either, and counting them as records would make a perfectly good load look like it had dropped some.
   */
  @Test
  void csvCountsHeadersAndSeparatorsAsSkipped() throws Exception {
    final String payload = """
        @type,@class,@id,id
        vertex,V1,c1,200
        vertex,V1,c2,201

        ---
        @type,@class,@from,@to
        edge,E1,c1,c2
        """;

    final CsvBatchRecordStream stream = new CsvBatchRecordStream(stream(payload));
    final int records = drain(stream, 3);

    assertThat(records).isEqualTo(3);
    assertThat(stream.getLinesRead()).isEqualTo(7);
    // header, blank, ---, header
    assertThat(stream.getLinesSkipped()).isEqualTo(4);
    assertThat(stream.getLinesRead() - stream.getLinesSkipped()).isEqualTo(records);
  }

  @Test
  void anEmptyPayloadReadsAndSkipsNothing() throws Exception {
    final JsonlBatchRecordStream stream = new JsonlBatchRecordStream(stream(""));

    assertThat(stream.hasNext()).isFalse();
    assertThat(stream.getLinesRead()).isZero();
    assertThat(stream.getLinesSkipped()).isZero();
  }

  private static int drain(final BatchRecordStream stream, final int expected) throws IOException {
    int records = 0;
    while (stream.hasNext()) {
      stream.next();
      records++;
    }
    assertThat(records).isEqualTo(expected);
    return records;
  }

  private static ByteArrayInputStream stream(final String payload) {
    return new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));
  }
}
