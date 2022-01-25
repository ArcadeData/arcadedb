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
package com.arcadedb.integration.importer;

import com.arcadedb.database.DatabaseFactory;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

public class Parser {
  private final Source            source;
  private       InputStream       is;
  private final InputStreamReader reader;
  private final long              limit;
  private final AtomicLong        position = new AtomicLong();
  private final long              total;
  private       char              currentChar;
  private final boolean           compressed;

  public Parser(final Source source, final long limit) throws IOException {
    this.source = source;
    this.limit = limit;
    resetInput();

    this.compressed = source.compressed;
    this.total = source.totalSize;

    this.reader = new InputStreamReader(this.is, DatabaseFactory.getDefaultCharset());
    this.is.mark(0);
  }

  public char getCurrentChar() {
    return currentChar;
  }

  public char nextChar() throws IOException {
    position.incrementAndGet();
    currentChar = (char) reader.read();
    return currentChar;
  }

  public void mark() {
    is.mark(0);
  }

  public void reset() throws IOException {
    currentChar = 0;
    position.set(0);
    source.reset();
    resetInput();
  }

  public boolean isAvailable() throws IOException {
    if (limit > 0)
      return position.get() < limit && is.available() > 0;
    return is.available() > 0;
  }

  public InputStream getInputStream() {
    return is;
  }

  public long getPosition() {
    return position.get();
  }

  public long getTotal() {
    return limit > 0 ? Math.min(limit, total) : total;
  }

  public boolean isCompressed() {
    return compressed;
  }

  public Source getSource() {
    return source;
  }

  private void resetInput() {
    this.is = new BufferedInputStream(source.inputStream) {
      @Override
      public synchronized int read() throws IOException {
        position.incrementAndGet();
        return super.read();
      }

      @Override
      public synchronized int read(final byte[] b) throws IOException {
        if (limit > 0 && position.get() > limit)
          throw new EOFException();

        final int res = super.read(b);
        position.addAndGet(res);
        return res;
      }

      @Override
      public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
        if (limit > 0 && position.get() > limit)
          throw new EOFException();

        int res = super.read(b, off, len);
        position.addAndGet(res);
        return res;
      }

      @Override
      public synchronized int available() throws IOException {
        if (limit > 0 && position.get() > limit)
          return 0;

        return super.available();
      }

      @Override
      public synchronized void reset() {
        pos = 0;
        position.set(0);
      }
    };
  }
}
