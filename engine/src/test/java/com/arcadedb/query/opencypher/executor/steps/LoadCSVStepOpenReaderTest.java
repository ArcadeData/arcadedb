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
package com.arcadedb.query.opencypher.executor.steps;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for {@code LOAD CSV} opening a malformed archive (issue #7122): the raw stream is already
 * open by the time the {@code .gz}/{@code .zip} wrapper rejects the header, and the caller only ever closes the
 * {@link BufferedReader} it never received - so every failed open used to strand one file descriptor (or, for an
 * {@code http(s)} source, one socket).
 */
class LoadCSVStepOpenReaderTest {

  /** An InputStream that records whether it was closed, so the ownership handover is directly observable. */
  private static final class TrackingInputStream extends FilterInputStream {
    private boolean closed;

    private TrackingInputStream(final byte[] content) {
      super(new ByteArrayInputStream(content));
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }

  @Test
  void aCorruptGzipHeaderDoesNotStrandTheRawStream() {
    // Not gzip: GZIPInputStream rejects the magic number from inside its own constructor, with the stream
    // it was handed already open.
    final TrackingInputStream raw = new TrackingInputStream("id,name\n1,alice\n".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> LoadCSVStep.decompressingReader("file:///data.csv.gz", raw))
        .isInstanceOf(IOException.class);

    assertThat(raw.closed).as("a rejected archive header must not cost a file descriptor").isTrue();
  }

  @Test
  void anEmptyZipDoesNotStrandTheRawStream() throws IOException {
    final TrackingInputStream raw = new TrackingInputStream(emptyZipArchive());

    assertThatThrownBy(() -> LoadCSVStep.decompressingReader("file:///data.csv.zip", raw))
        // An IOException, so the caller's "Error opening CSV file: <url>" arm reports it with the url the way
        // it reports every other open failure, instead of letting it escape uncontextualised.
        .isInstanceOf(IOException.class)
        .hasMessageContaining("ZIP file is empty");

    assertThat(raw.closed).as("an archive with no entry must not cost a file descriptor").isTrue();
  }

  @Test
  void aPlainStreamIsHandedToTheReaderStillOpen() throws IOException {
    final TrackingInputStream raw = new TrackingInputStream("id,name\n1,alice\n".getBytes(StandardCharsets.UTF_8));

    try (final BufferedReader reader = LoadCSVStep.decompressingReader("file:///data.csv", raw)) {
      assertThat(raw.closed).as("the success path must hand the caller a usable reader").isFalse();
      assertThat(reader.readLine()).isEqualTo("id,name");
    }

    // Closing the reader is what closes the underlying stream, as before.
    assertThat(raw.closed).isTrue();
  }

  private static byte[] emptyZipArchive() throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    // A ZipOutputStream closed without a single entry writes only the end-of-central-directory record, which is
    // exactly what getNextEntry() answers null for.
    new ZipOutputStream(bytes).close();
    return bytes.toByteArray();
  }
}
