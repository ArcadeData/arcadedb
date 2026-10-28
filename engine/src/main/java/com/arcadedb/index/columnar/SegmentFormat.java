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
package com.arcadedb.index.columnar;

/**
 * Constants and file names of a columnar segment file {@code seg-<id>.acs}. Every block of the file is a plan 2 {@code BlockHeader}
 * followed by its columns; all multi-byte fields written by the segment classes are big-endian, the numeric codecs keep their own
 * little-endian layout.
 * <pre>
 * file header block   FILE_MAGIC, columns [meta: segmentId long, maxRecordsInPage int, blockRows int, rowCount long][definitions JSON]
 * row blocks          BLOCK_MAGIC, 3 sub-columns per column: [value presence bits][values][list offsets]
 * presence blocks     BLOCK_MAGIC, up to PAGES_PER_PRESENCE_BLOCK covered pages: [pageNumbers int[]][pageRowStart int[]][stripes long[]]
 * page version blocks BLOCK_MAGIC, one per presence block: [versions long[]]
 * footer              minPage int, maxPage int, entry count int, directory entries (kind int, first int, offset int, size int),
 *                     then per column min long, max long
 * trailer             footerOffset int, footerCrc32 int, FILE_MAGIC int
 * </pre>
 * The footer directory locates every block, so readers never depend on the order of the sections; the writer streams row blocks
 * and appends presence and page versions once all positions are known.
 */
final class SegmentFormat {
  static final int FILE_MAGIC  = 0x41435347; // "ACSG"
  static final int BLOCK_MAGIC = 0x41434231; // "ACB1"
  static final int VERSION     = 1;

  /** Directory kinds; {@code first} is the first covered page for presence and page versions, the first row for row blocks. */
  static final int KIND_PRESENCE      = 0;
  static final int KIND_PAGE_VERSIONS = 1;
  static final int KIND_ROWS          = 2;

  static final int PAGES_PER_PRESENCE_BLOCK = 1024;
  /** Sub-columns per column in a row block: value presence bits, values, list offsets. */
  static final int SUB_COLUMNS              = 3;

  static final int HEADER_META_BYTES  = 8 + 4 + 4 + 8;
  static final int DIRECTORY_ENTRY    = 16;
  static final int FOOTER_FIXED_BYTES = 4 + 4 + 4;
  static final int TRAILER_BYTES      = 12;

  private SegmentFormat() {
  }

  static String segmentFileName(final long segmentId) {
    return String.format("seg-%016x.acs", segmentId);
  }

  static String deadFileName(final long segmentId, final int version) {
    return String.format("seg-%016x.dead.%d", segmentId, version);
  }

  /** {@code dict-<indexFileId>.<generation>.dic}: one generation of a column's dictionary in its bucket directory. */
  static String dictionaryFileName(final int indexFileId, final int generation) {
    return "dict-" + indexFileId + "." + generation + ".dic";
  }
}
