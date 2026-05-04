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
package com.arcadedb.index.sparsevector;

/**
 * On-disk format constants for {@code .sparseseg} sealed segments.
 * <p>
 * See {@code docs/sparse-vector-storage-design.md} for the full layout. This class is the
 * single source of truth for byte-level offsets and sentinel values shared between the writer
 * and reader.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SegmentFormat {
  public static final long MAGIC                 = 0x4153505630303031L; // "ASPV0001"
  public static final int  FORMAT_VERSION        = 1;
  public static final int  DEFAULT_PAGE_SIZE     = 65536;               // 64 KiB
  public static final int  DEFAULT_BLOCK_SIZE    = 128;                 // postings per block
  public static final int  DEFAULT_SKIP_STRIDE   = 8;                   // blocks per skip entry
  public static final int  MIN_BLOCK_SIZE        = 16;
  public static final int  MAX_BLOCK_SIZE        = 4096;
  public static final int  MIN_SKIP_STRIDE       = 1;
  public static final int  MAX_SKIP_STRIDE       = 1024;

  // File header layout (page 0).
  public static final int  HEADER_OFFSET_MAGIC               = 0;
  public static final int  HEADER_OFFSET_FORMAT_VERSION      = 8;
  public static final int  HEADER_OFFSET_PAGE_SIZE           = 12;
  public static final int  HEADER_OFFSET_BLOCK_SIZE          = 16;
  public static final int  HEADER_OFFSET_SKIP_STRIDE         = 20;
  public static final int  HEADER_OFFSET_WEIGHT_QUANTIZATION = 24;
  public static final int  HEADER_OFFSET_RID_COMPRESSION     = 25;
  public static final int  HEADER_OFFSET_RESERVED            = 26;     // 14 bytes reserved
  public static final int  HEADER_OFFSET_MANIFEST_OFFSET     = 40;
  public static final int  HEADER_OFFSET_TOTAL_POSTINGS      = 48;
  public static final int  HEADER_OFFSET_TOTAL_DIMS          = 56;
  public static final int  HEADER_OFFSET_CREATED_AT          = 60;
  public static final int  HEADER_OFFSET_CRC32               = 68;
  public static final int  HEADER_SIZE                       = 72;

  // RID is (int bucketId, long offset). 12 bytes raw.
  public static final int  RID_SIZE_BYTES = 12;

  // int8 quantization sentinel for tombstones (signed -128 / unsigned 0x80).
  public static final byte INT8_TOMBSTONE_SENTINEL = (byte) 0x80;
  // int8 quantization uses 254 levels (0..253) skipping the sentinel byte 0x80.
  public static final int  INT8_LEVELS             = 254;

  // fp16 sentinel for tombstones.
  public static final short FP16_TOMBSTONE_SENTINEL = (short) 0xFE00;

  // Skip entry: first_RID (12B) + max_weight_to_end (4B) + block_index (4B) = 20 bytes.
  public static final int  SKIP_ENTRY_SIZE = RID_SIZE_BYTES + 4 + 4;

  // Per-block header layout: first_RID + last_RID + posting_count(short) + max_weight + weight_min + weight_max + has_tombstones(byte) + reserved(byte).
  public static final int  BLOCK_HEADER_SIZE = RID_SIZE_BYTES + RID_SIZE_BYTES + 2 + 4 + 4 + 4 + 1 + 1;

  public enum WeightQuantization {
    FP32((byte) 0),
    INT8((byte) 1),
    FP16((byte) 2);

    private final byte code;

    WeightQuantization(final byte code) {
      this.code = code;
    }

    public byte code() {
      return code;
    }

    public static WeightQuantization fromCode(final byte code) {
      for (final WeightQuantization q : values())
        if (q.code == code)
          return q;
      throw new IllegalArgumentException("Unknown weight quantization code: " + code);
    }
  }

  public enum RidCompression {
    RAW((byte) 0),
    VARINT_DELTA((byte) 1);

    private final byte code;

    RidCompression(final byte code) {
      this.code = code;
    }

    public byte code() {
      return code;
    }

    public static RidCompression fromCode(final byte code) {
      for (final RidCompression c : values())
        if (c.code == code)
          return c;
      throw new IllegalArgumentException("Unknown RID compression code: " + code);
    }
  }

  private SegmentFormat() {
    // utility class
  }
}
