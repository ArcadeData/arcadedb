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
package com.arcadedb.index.vector;

import com.arcadedb.index.IndexException;
import io.github.jbellis.jvector.disk.IndexWriter;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.quantization.MutablePQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9796: {@code LSMVectorIndexPQFile.writePQ} rewrote the LIVE {@code .vecpq} file in place
 * ({@code TRUNCATE_EXISTING}), so the previous, complete PQ data was destroyed before the first byte of the
 * replacement was written and anything that interrupted the stream left a fragment that {@code exists()} reported as
 * present while {@code loadPQ()} could not read it.
 * <p>
 * The tests assert the RULE, not a round trip: a rewrite that fails at a chosen point leaves the previous file in
 * place, byte for byte, and still loadable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9796PQFileAtomicRewriteTest {

  private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

  private static final int DIMENSIONS = 16;
  private static final int SUBSPACES  = 4;
  private static final int CLUSTERS   = 16;

  @TempDir
  Path tempDir;

  @Test
  void aFailedRewriteLeavesThePreviousFileInPlaceAndLoadable() throws IOException {
    final String base = tempDir.resolve("index").toString();
    final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

    final Fixture first = new Fixture(1, 300);
    pqFile.writePQ(first.pq, first.codes);
    final byte[] firstBytes = Files.readAllBytes(pqFile.getFilePath());
    assertThat(firstBytes.length).isGreaterThan(0);

    // The second write dies after the codebook and the first stretch of codes have been streamed: the mid-stream
    // failure that used to leave a fragment where the complete first file had been.
    final Fixture second = new Fixture(2, 600);
    final MutablePQVectors failing = new FailingPQVectors(second.pq, 600, 512);
    assertThatThrownBy(() -> pqFile.writePQ(second.pq, failing)).isInstanceOf(IndexException.class);

    assertThat(Files.readAllBytes(pqFile.getFilePath()))
        .as("a rewrite that fails must leave the previous PQ file byte-identical, not a truncated fragment")
        .isEqualTo(firstBytes);
    assertThat(listTemporaries()).as("the failed attempt must not leave its temporary file behind").isEmpty();

    final LSMVectorIndexPQFile reopened = new LSMVectorIndexPQFile(base);
    assertThat(reopened.loadPQ()).as("the previous file must still load").isTrue();
    assertThat(reopened.getPQVectors().count()).isEqualTo(first.codes.count());
    assertThat(reopened.hasUsableData()).isTrue();

    // The handler that failed keeps serving what it already held: nothing was published.
    assertThat(pqFile.getPQVectors()).isSameAs(first.codes);
    assertThat(pqFile.getProductQuantization()).isSameAs(first.pq);
  }

  @Test
  void anErrorDuringTheRewriteLeavesThePreviousFileAndNoTemporary() throws IOException {
    final String base = tempDir.resolve("index").toString();
    final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

    final Fixture first = new Fixture(1, 300);
    pqFile.writePQ(first.pq, first.codes);
    final byte[] firstBytes = Files.readAllBytes(pqFile.getFilePath());

    // An OutOfMemoryError while encoding or streaming a large file is not an Exception: it used to skip the cleanup
    final Fixture second = new Fixture(2, 600);
    final MutablePQVectors failing = new FailingPQVectors(second.pq, 600, 512, new OutOfMemoryError("injected"));
    assertThatThrownBy(() -> pqFile.writePQ(second.pq, failing)).isInstanceOf(OutOfMemoryError.class);

    assertThat(Files.readAllBytes(pqFile.getFilePath())).isEqualTo(firstBytes);
    assertThat(listTemporaries()).as("an Error must not leave the temporary file behind either").isEmpty();
    assertThat(pqFile.getPQVectors()).isSameAs(first.codes);
  }

  @Test
  void aSuccessfulRewriteReplacesThePreviousFileAndPublishesTheNewOne() throws IOException {
    final String base = tempDir.resolve("index").toString();
    final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

    final Fixture first = new Fixture(1, 300);
    pqFile.writePQ(first.pq, first.codes);

    final Fixture second = new Fixture(2, 600);
    pqFile.writePQ(second.pq, second.codes);

    assertThat(pqFile.getPQVectors()).isSameAs(second.codes);
    assertThat(listTemporaries()).isEmpty();

    final LSMVectorIndexPQFile reopened = new LSMVectorIndexPQFile(base);
    assertThat(reopened.loadPQ()).isTrue();
    assertThat(reopened.getPQVectors().count()).isEqualTo(600);
  }

  @Test
  void anInterruptDuringTheRewriteDoesNotLeaveAFragment() throws IOException {
    final String base = tempDir.resolve("index").toString();
    final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

    final Fixture first = new Fixture(1, 300);
    pqFile.writePQ(first.pq, first.codes);

    // LSMVectorIndex builds on a pool it cancels with shutdownNow(), which interrupts the thread that is writing the
    // PQ data. A pending interrupt closes a FileChannel on its first I/O (ClosedByInterruptException).
    final Fixture second = new Fixture(2, 600);
    Thread.currentThread().interrupt();
    boolean stillInterrupted;
    try {
      pqFile.writePQ(second.pq, second.codes);
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    assertThat(stillInterrupted).as("the caller must still be told it was interrupted").isTrue();
    assertThat(listTemporaries()).isEmpty();

    // Whatever the write decided, the file on disk is complete: the new data, never a fragment.
    final LSMVectorIndexPQFile reopened = new LSMVectorIndexPQFile(base);
    assertThat(reopened.loadPQ()).as("the file on disk must load after an interrupted rewrite").isTrue();
    assertThat(reopened.getPQVectors().count()).isEqualTo(600);
  }

  @Test
  void theWriterCompletesItsStreamAcrossAnInterrupt() throws IOException {
    final Path path = tempDir.resolve("stream.bin");
    final byte[] payload = new byte[100_000];
    new Random(9796).nextBytes(payload);

    Thread.currentThread().interrupt();
    boolean stillInterrupted;
    try (SimpleFileIndexWriter writer = new SimpleFileIndexWriter(path)) {
      writer.write(payload);
      writer.writeLong(42L);
      assertThat(writer.position()).isEqualTo(payload.length + 8L);
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    assertThat(stillInterrupted).isTrue();
    final byte[] written = Files.readAllBytes(path);
    assertThat(written).hasSize(payload.length + 8);
    assertThat(Arrays.copyOf(written, payload.length)).isEqualTo(payload);
  }

  @Test
  void aLeftoverTemporaryFromACrashedWriteIsRemoved() throws IOException {
    final String base = tempDir.resolve("index").toString();
    final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

    final Path leftover = tempDir.resolve("index." + LSMVectorIndexPQFile.FILE_EXT + ".deadbeef.tmp");
    Files.write(leftover, new byte[] { 1, 2, 3 });
    final Path unrelated = tempDir.resolve("other.tmp");
    Files.write(unrelated, new byte[] { 4 });

    final Fixture fixture = new Fixture(1, 300);
    pqFile.writePQ(fixture.pq, fixture.codes);

    assertThat(leftover).as("a process killed between the write and the rename leaves a temporary nothing else removes")
        .doesNotExist();
    assertThat(unrelated).as("only this file's own temporaries are touched").exists();

    Files.write(leftover, new byte[] { 1, 2, 3 });
    pqFile.deletePQFile();
    assertThat(leftover).doesNotExist();
    assertThat(pqFile.getFilePath()).doesNotExist();
  }

  @Test
  void aFileNameWithGlobMetacharactersStillGetsItsLeftoversRemoved() throws IOException {
    // A directory-stream glob built from the file name would throw (unclosed '[' and '{') or match the wrong files; the
    // failure is swallowed, so the leftovers would silently never be cleaned
    final String base = tempDir.resolve("in[d{ex*?").toString();
    final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);

    final Path leftover = tempDir.resolve("in[d{ex*?." + LSMVectorIndexPQFile.FILE_EXT + ".deadbeef.tmp");
    Files.write(leftover, new byte[] { 1, 2, 3 });
    final Path neighbour = tempDir.resolve("inXdYex." + LSMVectorIndexPQFile.FILE_EXT + ".deadbeef.tmp");
    Files.write(neighbour, new byte[] { 4 });

    final Fixture fixture = new Fixture(1, 300);
    pqFile.writePQ(fixture.pq, fixture.codes);

    assertThat(leftover).doesNotExist();
    assertThat(neighbour).as("a name the glob would have matched is not this file's temporary").exists();
  }

  @Test
  void deletePQFileReportsWhetherTheFileIsReallyGone() throws IOException {
    final String base = tempDir.resolve("index").toString();
    final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);
    assertThat(pqFile.deletePQFile()).as("nothing to delete is a file that is gone").isTrue();

    final Fixture fixture = new Fixture(1, 300);
    pqFile.writePQ(fixture.pq, fixture.codes);
    assertThat(pqFile.deletePQFile()).isTrue();
    assertThat(pqFile.getFilePath()).doesNotExist();

    // Something that cannot be deleted: a non-empty directory under the file's name. The caller that drops a stale file
    // has to be told, because a stale file left on disk is paired with the next graph on the following open.
    Files.createDirectory(pqFile.getFilePath());
    Files.write(pqFile.getFilePath().resolve("child"), new byte[] { 1 });
    assertThat(pqFile.deletePQFile()).as("the delete failed: the file is still there").isFalse();
    assertThat(pqFile.exists()).isTrue();
  }

  @Test
  void anUnloadableFileIsNotUsableDataEvenThoughItExists() throws IOException {
    final String base = tempDir.resolve("index").toString();
    final LSMVectorIndexPQFile pqFile = new LSMVectorIndexPQFile(base);
    assertThat(pqFile.hasUsableData()).as("no file, no data").isFalse();

    // The fragment the pre-fix writer left behind: the start of a real file.
    final Fixture fixture = new Fixture(1, 300);
    pqFile.writePQ(fixture.pq, fixture.codes);
    final byte[] whole = Files.readAllBytes(pqFile.getFilePath());
    Files.write(pqFile.getFilePath(), Arrays.copyOf(whole, whole.length / 3));

    final LSMVectorIndexPQFile reopened = new LSMVectorIndexPQFile(base);
    assertThat(reopened.loadPQ()).isFalse();
    assertThat(reopened.exists()).as("the fragment is on disk").isTrue();
    assertThat(reopened.hasUsableData())
        .as("but \"is there PQ data?\" must give the same answer as isPQReady() for it - the open-time decision keys "
            + "on this")
        .isFalse();
  }

  private List<Path> listTemporaries() throws IOException {
    final List<Path> temporaries = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(tempDir, "*.tmp")) {
      stream.forEach(temporaries::add);
    }
    return temporaries;
  }

  /** A trained codebook and the codes of {@code count} random vectors. */
  static final class Fixture {
    final ProductQuantization pq;
    final MutablePQVectors    codes;

    Fixture(final long seed, final int count) {
      final Random random = new Random(seed);
      final List<VectorFloat<?>> vectors = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final float[] values = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          values[d] = random.nextFloat();
        vectors.add(VTS.createFloatVector(values));
      }
      final ListRandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectors, DIMENSIONS);
      this.pq = ProductQuantization.compute(ravv, SUBSPACES, CLUSTERS, false);
      this.codes = new MutablePQVectors(pq);
      for (int i = 0; i < count; i++)
        codes.encodeAndSet(i, vectors.get(i));
    }
  }

  /** Codes that fail after streaming part of themselves, like a full disk or an I/O error halfway through. */
  static final class FailingPQVectors extends MutablePQVectors {
    private final int       vectorCount;
    private final int       failAfterBytes;
    private final Throwable failure;

    FailingPQVectors(final ProductQuantization pq, final int vectorCount, final int failAfterBytes) {
      this(pq, vectorCount, failAfterBytes, new IOException("injected failure after " + failAfterBytes + " bytes of codes"));
    }

    FailingPQVectors(final ProductQuantization pq, final int vectorCount, final int failAfterBytes, final Throwable failure) {
      super(pq);
      this.vectorCount = vectorCount;
      this.failAfterBytes = failAfterBytes;
      this.failure = failure;
    }

    @Override
    public int count() {
      return vectorCount;
    }

    @Override
    public void write(final IndexWriter out, final int version) throws IOException {
      out.write(new byte[failAfterBytes]);
      if (failure instanceof IOException e)
        throw e;
      if (failure instanceof RuntimeException e)
        throw e;
      throw (Error) failure;
    }
  }
}
