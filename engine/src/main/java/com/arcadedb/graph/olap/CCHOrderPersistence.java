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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DataEncryption;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * Persists the vertex order of a {@link ContractionHierarchy} next to its view's persisted CSR (issue #9437), so that a
 * database reopened with nothing committed in between contracts the hierarchy in the order it already had instead of
 * computing a new one - the order is by far the most expensive part of a hierarchy to build.
 * <p>
 * The order is expressed in the dense id space of the CSR it was computed for, so it carries the same freshness
 * certificate as the CSR file (the database's last committed transaction id when that CSR was scanned) and is used only
 * against a CSR restored with that very certificate. Encrypted like the CSR file when the database encrypts its data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CCHOrderPersistence {
  private static final int    MAGIC          = 0x4343484F; // "CCHO"
  private static final int    FORMAT_VERSION = 1;
  private static final String FILE_EXTENSION = ".cchorder";

  private CCHOrderPersistence() {
  }

  /**
   * The file of the hierarchy for {@code weightProperty} over {@code edgeTypes} on {@code viewName}. Every name is encoded
   * as the CSR file's view name is, so none of them can step out of the database directory.
   */
  static File fileFor(final Database database, final String viewName, final String weightProperty, final String[] edgeTypes) {
    final File csr = GraphAnalyticalViewCSRPersistence.fileFor(database, viewName);
    final String encoding = database.getSchema().getEncoding();
    final StringBuilder name = new StringBuilder(csr.getName()).append('.').append(FileUtils.encode(weightProperty, encoding));
    if (edgeTypes != null)
      for (final String type : edgeTypes)
        name.append('.').append(FileUtils.encode(type, encoding));
    return new File(csr.getParentFile(), name.append(FILE_EXTENSION).toString());
  }

  static void save(final Database database, final File file, final long asOfTransactionId, final int[] order)
      throws IOException {
    final ByteArrayOutputStream payloadBuffer = new ByteArrayOutputStream(order.length * 4 + 8);
    try (final DataOutputStream payloadOut = new DataOutputStream(payloadBuffer)) {
      payloadOut.writeInt(order.length);
      for (final int node : order)
        payloadOut.writeInt(node);
    }
    final DataEncryption encryption = ((DatabaseInternal) database).getDataEncryption();
    final byte[] payload = encryption != null ? encryption.encrypt(payloadBuffer.toByteArray()) : payloadBuffer.toByteArray();
    final CRC32 crc = new CRC32();
    crc.update(payload);

    final File tmp = new File(file.getParentFile(), file.getName() + "." + Long.toHexString(System.nanoTime()) + ".tmp");
    try (final DataOutputStream out = new DataOutputStream(new FileOutputStream(tmp))) {
      out.writeInt(MAGIC);
      out.writeInt(FORMAT_VERSION);
      out.writeLong(asOfTransactionId);
      out.writeBoolean(encryption != null);
      out.writeInt(payload.length);
      out.write(payload);
      out.writeLong(crc.getValue());
    } catch (final IOException | RuntimeException e) {
      Files.deleteIfExists(tmp.toPath());
      throw e;
    }
    try {
      try {
        Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (final IOException e) {
      Files.deleteIfExists(tmp.toPath());
      throw e;
    }
  }

  /**
   * The persisted order, or null when there is none, it was computed for another CSR than the one certified by
   * {@code asOfTransactionId}, or it cannot be read back intact (in which case the file is removed).
   */
  static int[] load(final Database database, final File file, final long asOfTransactionId) {
    if (asOfTransactionId < 0 || !file.isFile())
      return null;
    try (final DataInputStream in = new DataInputStream(new FileInputStream(file))) {
      if (in.readInt() != MAGIC || in.readInt() != FORMAT_VERSION || in.readLong() != asOfTransactionId)
        return null;
      final boolean encrypted = in.readBoolean();
      final int length = in.readInt();
      if (length < 0 || length > file.length())
        throw new IOException("declared payload length " + length + " exceeds the file's size");
      final byte[] payload = new byte[length];
      in.readFully(payload);
      final CRC32 crc = new CRC32();
      crc.update(payload);
      if (crc.getValue() != in.readLong())
        throw new IOException("checksum mismatch");

      final DataEncryption encryption = ((DatabaseInternal) database).getDataEncryption();
      if (encrypted && encryption == null)
        throw new IOException("the order is encrypted but the database has no encryption configured");
      final byte[] raw = encrypted ? encryption.decrypt(payload) : payload;
      try (final DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(raw))) {
        final int count = payloadIn.readInt();
        if (count < 0 || (long) count * 4 > raw.length)
          throw new IOException("declared node count " + count + " exceeds the payload");
        final int[] order = new int[count];
        for (int i = 0; i < count; i++)
          order[i] = payloadIn.readInt();
        return order;
      }
    } catch (final IOException | RuntimeException | OutOfMemoryError e) {
      LogManager.instance().log(CCHOrderPersistence.class, Level.WARNING,
          "Persisted contraction hierarchy order '%s' is unusable (%s), discarding it", null, file.getName(), e.toString());
      delete(file);
      return null;
    }
  }

  static void delete(final File file) {
    try {
      Files.deleteIfExists(file.toPath());
    } catch (final IOException e) {
      LogManager.instance().log(CCHOrderPersistence.class, Level.FINE, "Could not delete '%s': %s", null, file.getName(),
          e.getMessage());
    }
  }
}
