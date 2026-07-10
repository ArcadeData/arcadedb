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
package com.arcadedb.engine;

import com.arcadedb.database.BasicDatabase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4321: PaginatedComponentFile.write/read must loop
 * until the entire page buffer is transferred, guarding against short-write and
 * short-read returns from the underlying FileChannel.
 */
class PaginatedComponentFileRoundTripTest {

  private static final int PAGE_SIZE = 1024;
  private static final int FILE_ID   = 1;

  @TempDir
  Path tempDir;

  private PaginatedComponentFile pcf;
  private BasicDatabase          db;

  @BeforeEach
  void setUp() throws IOException {
    db = Mockito.mock(BasicDatabase.class);
    final String filePath = tempDir.resolve("page." + FILE_ID + "." + PAGE_SIZE + ".v0.arc").toString();
    pcf = new PaginatedComponentFile(filePath, ComponentFile.MODE.READ_WRITE);
  }

  @AfterEach
  void tearDown() {
    if (pcf != null)
      pcf.close();
  }

  @Test
  void writeThenReadRoundTripPreservesContent() throws Exception {
    final PageId     pageId = new PageId(db, FILE_ID, 0);
    final byte[]     data   = new byte[PAGE_SIZE];
    Arrays.fill(data, (byte) 0x42);
    final MutablePage page = new MutablePage(pageId, PAGE_SIZE, data, 1, PAGE_SIZE);

    pcf.write(page);

    final CachedPage readPage = new CachedPage((PageManager) null, pageId, PAGE_SIZE);
    pcf.read(readPage);

    final ByteBuffer buf      = readPage.getByteBuffer();
    buf.rewind();
    final byte[] readData = new byte[PAGE_SIZE];
    buf.get(readData);

    assertThat(readData).isEqualTo(data);
  }

  @Test
  void multiplePagesAreAddressedIndependently() throws Exception {
    final byte[] dataPage0 = new byte[PAGE_SIZE];
    Arrays.fill(dataPage0, (byte) 0xAA);
    final byte[] dataPage1 = new byte[PAGE_SIZE];
    Arrays.fill(dataPage1, (byte) 0xBB);

    final PageId      pageId0 = new PageId(db, FILE_ID, 0);
    final MutablePage page0   = new MutablePage(pageId0, PAGE_SIZE, dataPage0, 0, PAGE_SIZE);
    pcf.write(page0);

    final PageId      pageId1 = new PageId(db, FILE_ID, 1);
    final MutablePage page1   = new MutablePage(pageId1, PAGE_SIZE, dataPage1, 0, PAGE_SIZE);
    pcf.write(page1);

    // Read out of order to confirm positional addressing
    final CachedPage readPage1 = new CachedPage((PageManager) null, pageId1, PAGE_SIZE);
    pcf.read(readPage1);
    final CachedPage readPage0 = new CachedPage((PageManager) null, pageId0, PAGE_SIZE);
    pcf.read(readPage0);

    final ByteBuffer buf0      = readPage0.getByteBuffer();
    buf0.rewind();
    final byte[] read0 = new byte[PAGE_SIZE];
    buf0.get(read0);
    assertThat(read0).isEqualTo(dataPage0);

    final ByteBuffer buf1      = readPage1.getByteBuffer();
    buf1.rewind();
    final byte[] read1 = new byte[PAGE_SIZE];
    buf1.get(read1);
    assertThat(read1).isEqualTo(dataPage1);
  }

  @Test
  void overwrittenPageReflectsLatestContent() throws Exception {
    final PageId pageId = new PageId(db, FILE_ID, 0);

    final byte[] firstData = new byte[PAGE_SIZE];
    Arrays.fill(firstData, (byte) 0x11);
    pcf.write(new MutablePage(pageId, PAGE_SIZE, firstData, 0, PAGE_SIZE));

    final byte[] secondData = new byte[PAGE_SIZE];
    Arrays.fill(secondData, (byte) 0x22);
    pcf.write(new MutablePage(pageId, PAGE_SIZE, secondData, 1, PAGE_SIZE));

    final CachedPage readPage = new CachedPage((PageManager) null, pageId, PAGE_SIZE);
    pcf.read(readPage);

    final ByteBuffer buf      = readPage.getByteBuffer();
    buf.rewind();
    final byte[] readData = new byte[PAGE_SIZE];
    buf.get(readData);

    assertThat(readData).isEqualTo(secondData);
  }
}
