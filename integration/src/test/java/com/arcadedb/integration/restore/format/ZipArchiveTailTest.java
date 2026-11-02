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
package com.arcadedb.integration.restore.format;

import com.arcadedb.integration.restore.RestoreException;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The end-record check of issue #11041 on archives the backup itself never produces: a comment, the ZIP64 records, a stream
 * long enough to wrap the tail buffer many times, and the CTR counter arithmetic that lets an encrypted tail be decrypted
 * without the front.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ZipArchiveTailTest {

  @Test
  void anArchiveWithACommentIsTerminated() throws Exception {
    final byte[] archive = archive(3, "a comment that holds PK\u0005\u0006 the end signature");
    assertThat(walk(archive)).isEqualTo(3);
  }

  /** More entries than the 16-bit count holds: the ZIP64 end record carries the count, and the walk wraps the tail buffer. */
  @Test
  void aZip64ArchiveIsReadFromItsZip64EndRecord() throws Exception {
    final int entries = 0xFFFF + 10;
    assertThat(walk(archive(entries, null))).isEqualTo(entries);
  }

  @Test
  void anArchiveCutAnywhereInItsEndRecordsIsRefused() throws Exception {
    final byte[] archive = archive(5, null);
    for (int cut = 1; cut <= 22; cut++) {
      final byte[] truncated = Arrays.copyOf(archive, archive.length - cut);
      assertThatThrownBy(() -> walk(truncated)).as("cut %d", cut).isInstanceOf(RestoreException.class);
    }
  }

  @Test
  void anArchiveWithNoEntriesAndNoEndRecordIsRefused() {
    assertThatThrownBy(() -> ZipArchiveTail.declaredEntries(new byte[0], 0, 0, "empty", false)).isInstanceOf(RestoreException.class)
        .hasMessageContaining("is incomplete");
    assertThatThrownBy(() -> ZipArchiveTail.declaredEntries(new byte[10], 10, 10, "encrypted", true))
        .hasMessageContaining("wrong key");
  }

  /** The counter of block N is the IV plus N, carried across bytes, as the JDK's CTR mode advances it. */
  @Test
  void theCounterOfABlockMatchesTheCipherStream() throws Exception {
    final SecretKeySpec key = new SecretKeySpec(new byte[16], "AES");
    final byte[] iv = new byte[16];
    Arrays.fill(iv, 8, 16, (byte) 0xFF);
    iv[15] = (byte) 0xF0;
    final byte[] plain = new byte[16 * 600];
    for (int i = 0; i < plain.length; i++)
      plain[i] = (byte) (i * 31);

    final Cipher encrypt = Cipher.getInstance("AES/CTR/NoPadding");
    encrypt.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
    final byte[] encrypted = encrypt.doFinal(plain);

    for (final int block : new int[] { 0, 1, 15, 16, 17, 300, 599 }) {
      final Cipher decrypt = Cipher.getInstance("AES/CTR/NoPadding");
      decrypt.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(FullRestoreFormat.counterOfBlock(iv, block)));
      assertThat(decrypt.doFinal(encrypted, block * 16, encrypted.length - block * 16)).as("block %d", block)
          .isEqualTo(Arrays.copyOfRange(plain, block * 16, plain.length));
    }
  }

  /** The archive through the walk the sequential restore runs, and the check it ends with. */
  private static long walk(final byte[] archive) throws Exception {
    final ZipArchiveTail.TailCapturingInputStream capture = new ZipArchiveTail.TailCapturingInputStream(
        new ByteArrayInputStream(archive));
    final ZipInputStream zip = new ZipInputStream(capture, StandardCharsets.UTF_8);
    long read = 0;
    while (zip.getNextEntry() != null) {
      zip.readAllBytes();
      ++read;
    }
    capture.drain();
    assertThat(capture.length()).isEqualTo(archive.length);
    final long declared = ZipArchiveTail.declaredEntries(capture.tail(), capture.tailLength(), capture.length(), "test", false);
    ZipArchiveTail.checkEntriesRead(declared, read, "test");
    return declared;
  }

  private static byte[] archive(final int entries, final String comment) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ZipOutputStream zip = new ZipOutputStream(bytes, StandardCharsets.UTF_8)) {
      if (comment != null)
        zip.setComment(comment);
      for (int i = 0; i < entries; i++) {
        zip.putNextEntry(new ZipEntry("e" + i));
        zip.write(("content of " + i).getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
      }
    }
    return bytes.toByteArray();
  }
}
