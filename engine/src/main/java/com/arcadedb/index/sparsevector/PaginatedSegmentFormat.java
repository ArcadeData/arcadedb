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
 * Page-aligned segment format constants. These describe the byte layout that
 * {@link SparseSegmentBuilder} writes onto a {@link SparseSegmentComponent} and that
 * {@link PaginatedSegmentReader} parses back.
 * <p>
 * This is the page-component-backed counterpart of {@link SegmentFormat}, which described the
 * older raw-FileChannel layout. Block payload bytes (header + compressed RIDs + compressed
 * weights) reuse {@link SegmentFormat}'s constants verbatim - the only change between formats is
 * how blocks and trailers are <i>addressed</i>: file offsets in the legacy format, page locators
 * (page_num + offset_in_page) in this one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PaginatedSegmentFormat {

  // ----- Page 0: segment header -----
  // magic(8) format_version(4) block_size(4) skip_stride(4)
  // weight_quantization(1) rid_compression(1) reserved(2)
  // segment_id(8) total_postings(8) total_dims(4)
  // created_at(8) manifest_page_num(4) dim_index_page_num(4)
  // crc32(4)
  public static final int HEADER_OFFSET_MAGIC               = 0;
  public static final int HEADER_OFFSET_FORMAT_VERSION      = 8;
  public static final int HEADER_OFFSET_BLOCK_SIZE          = 12;
  public static final int HEADER_OFFSET_SKIP_STRIDE         = 16;
  public static final int HEADER_OFFSET_WEIGHT_QUANTIZATION = 20;
  public static final int HEADER_OFFSET_RID_COMPRESSION     = 21;
  public static final int HEADER_OFFSET_RESERVED            = 22; // 2 bytes
  public static final int HEADER_OFFSET_SEGMENT_ID          = 24;
  public static final int HEADER_OFFSET_TOTAL_POSTINGS      = 32;
  public static final int HEADER_OFFSET_TOTAL_DIMS          = 40;
  public static final int HEADER_OFFSET_CREATED_AT          = 44;
  public static final int HEADER_OFFSET_MANIFEST_PAGE_NUM   = 52;
  public static final int HEADER_OFFSET_DIM_INDEX_PAGE_NUM  = 56;
  public static final int HEADER_OFFSET_CRC32               = 60;
  public static final int HEADER_SIZE                       = 64;

  /** Block locator within a segment: page_num(4) + offset_in_page(2). */
  public static final int BLOCK_LOCATOR_SIZE = 4 + 2;

  /** Per-dim trailer header: dim_id(4) block_count(4) posting_count(4) df(4) global_max_weight(4). */
  public static final int DIM_TRAILER_HEADER_SIZE = 4 + 4 + 4 + 4 + 4;

  /** Per-dim-index entry: dim_id(4) trailer_page_num(4) trailer_offset_in_page(2). */
  public static final int DIM_INDEX_ENTRY_SIZE = 4 + 4 + 2;

  private PaginatedSegmentFormat() {
    // utility class
  }
}
