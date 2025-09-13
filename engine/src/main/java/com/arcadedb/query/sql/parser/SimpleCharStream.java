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
/* JavaCCOptions:STATIC=false,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import java.io.*;

/**
 * An implementation of interface CharStream, where the stream is assumed to
 * contain only ASCII characters (without unicode processing).
 */

public class SimpleCharStream {
  /**
   * Whether parser is static.
   */
  public static final boolean staticFlag = false;
  int bufsize;
  int available;
  int tokenBegin;
  /**
   * Position in buffer.
   */
  public    int   bufpos = -1;
  protected int[] bufline;
  protected int[] bufcolumn;

  protected int column = 0;
  protected int line   = 1;

  protected boolean prevCharIsCR = false;
  protected boolean prevCharIsLF = false;

  protected Reader inputStream;

  protected char[] buffer;
  protected int    maxNextCharInd = 0;
  protected int    inBuf          = 0;
  protected int    tabSize        = 8;

  protected void setTabSize(final int i) {
    tabSize = i;
  }

  protected int getTabSize(final int i) {
    return tabSize;
  }

  protected void ExpandBuff(final boolean wrapAround) {
    final char[] newbuffer = new char[bufsize + 2048];
    final int[] newbufline = new int[bufsize + 2048];
    final int[] newbufcolumn = new int[bufsize + 2048];

    try {
      if (wrapAround) {
        System.arraycopy(buffer, tokenBegin, newbuffer, 0, bufsize - tokenBegin);
        System.arraycopy(buffer, 0, newbuffer, bufsize - tokenBegin, bufpos);
        buffer = newbuffer;

        System.arraycopy(bufline, tokenBegin, newbufline, 0, bufsize - tokenBegin);
        System.arraycopy(bufline, 0, newbufline, bufsize - tokenBegin, bufpos);
        bufline = newbufline;

        System.arraycopy(bufcolumn, tokenBegin, newbufcolumn, 0, bufsize - tokenBegin);
        System.arraycopy(bufcolumn, 0, newbufcolumn, bufsize - tokenBegin, bufpos);
        bufcolumn = newbufcolumn;

        maxNextCharInd = (bufpos += (bufsize - tokenBegin));
      } else {
        System.arraycopy(buffer, tokenBegin, newbuffer, 0, bufsize - tokenBegin);
        buffer = newbuffer;

        System.arraycopy(bufline, tokenBegin, newbufline, 0, bufsize - tokenBegin);
        bufline = newbufline;

        System.arraycopy(bufcolumn, tokenBegin, newbufcolumn, 0, bufsize - tokenBegin);
        bufcolumn = newbufcolumn;

        maxNextCharInd = (bufpos -= tokenBegin);
      }
    } catch (final Throwable t) {
      throw new Error(t.getMessage(), t);
    }

    bufsize += 2048;
    available = bufsize;
    tokenBegin = 0;
  }

  protected void FillBuff() throws IOException {
    if (maxNextCharInd == available) {
      if (available == bufsize) {
        if (tokenBegin > 2048) {
          bufpos = maxNextCharInd = 0;
          available = tokenBegin;
        } else if (tokenBegin < 0)
          bufpos = maxNextCharInd = 0;
        else
          ExpandBuff(false);
      } else if (available > tokenBegin)
        available = bufsize;
      else if ((tokenBegin - available) < 2048)
        ExpandBuff(true);
      else
        available = tokenBegin;
    }

    final int i;
    try {
      if ((i = inputStream.read(buffer, maxNextCharInd, available - maxNextCharInd)) == -1) {
        inputStream.close();
        throw new IOException();
      } else
        maxNextCharInd += i;
    } catch (final IOException e) {
      --bufpos;
      backup(0);
      if (tokenBegin == -1)
        tokenBegin = bufpos;
      throw e;
    }
  }

  /**
   * Start.
   */
  public char BeginToken() throws IOException {
    tokenBegin = -1;
    final char c = readChar();
    tokenBegin = bufpos;

    return c;
  }

  protected void UpdateLineColumn(final char c) {
    column++;

    if (prevCharIsLF) {
      prevCharIsLF = false;
      line += (column = 1);
    } else if (prevCharIsCR) {
      prevCharIsCR = false;
      if (c == '\n') {
        prevCharIsLF = true;
      } else
        line += (column = 1);
    }

    switch (c) {
    case '\r':
      prevCharIsCR = true;
      break;
    case '\n':
      prevCharIsLF = true;
      break;
    case '\t':
      column--;
      column += (tabSize - (column % tabSize));
      break;
    default:
      break;
    }

    bufline[bufpos] = line;
    bufcolumn[bufpos] = column;
  }

  /**
   * Read a character.
   */
  public char readChar() throws IOException {
    if (inBuf > 0) {
      --inBuf;

      if (++bufpos == bufsize)
        bufpos = 0;

      return buffer[bufpos];
    }

    if (++bufpos >= maxNextCharInd)
      FillBuff();

    final char c = buffer[bufpos];

    UpdateLineColumn(c);
    return c;
  }

  @Deprecated
  /**
   * @deprecated
   * @see #getEndColumn
   */

  public int getColumn() {
    return bufcolumn[bufpos];
  }

  @Deprecated
  /**
   * @deprecated
   * @see #getEndLine
   */

  public int getLine() {
    return bufline[bufpos];
  }

  /**
   * Get token end column number.
   */
  public int getEndColumn() {
    return bufcolumn[bufpos];
  }

  /**
   * Get token end line number.
   */
  public int getEndLine() {
    return bufline[bufpos];
  }

  /**
   * Get token beginning column number.
   */
  public int getBeginColumn() {
    return bufcolumn[tokenBegin];
  }

  /**
   * Get token beginning line number.
   */
  public int getBeginLine() {
    return bufline[tokenBegin];
  }

  /**
   * Backup a number of characters.
   */
  public void backup(final int amount) {

    inBuf += amount;
    if ((bufpos -= amount) < 0)
      bufpos += bufsize;
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final Reader dstream, final int startline, final int startcolumn, final int buffersize) {
    inputStream = dstream;
    line = startline;
    column = startcolumn - 1;

    available = bufsize = buffersize;
    buffer = new char[buffersize];
    bufline = new int[buffersize];
    bufcolumn = new int[buffersize];
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final Reader dstream, final int startline, final int startcolumn) {
    this(dstream, startline, startcolumn, 4096);
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final Reader dstream) {
    this(dstream, 1, 1, 4096);
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final Reader dstream, final int startline, final int startcolumn, final int buffersize) {
    inputStream = dstream;
    line = startline;
    column = startcolumn - 1;

    if (buffer == null || buffersize != buffer.length) {
      available = bufsize = buffersize;
      buffer = new char[buffersize];
      bufline = new int[buffersize];
      bufcolumn = new int[buffersize];
    }
    prevCharIsLF = prevCharIsCR = false;
    tokenBegin = inBuf = maxNextCharInd = 0;
    bufpos = -1;
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final Reader dstream, final int startline, final int startcolumn) {
    ReInit(dstream, startline, startcolumn, 4096);
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final Reader dstream) {
    ReInit(dstream, 1, 1, 4096);
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final InputStream dstream, final String encoding, final int startline, final int startcolumn, final int buffersize)
      throws UnsupportedEncodingException {
    this(encoding == null ? new InputStreamReader(dstream) : new InputStreamReader(dstream, encoding), startline, startcolumn, buffersize);
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final InputStream dstream, final int startline, final int startcolumn, final int buffersize) {
    this(new InputStreamReader(dstream), startline, startcolumn, buffersize);
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final InputStream dstream, final String encoding, final int startline, final int startcolumn) throws UnsupportedEncodingException {
    this(dstream, encoding, startline, startcolumn, 4096);
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final InputStream dstream, final int startline, final int startcolumn) {
    this(dstream, startline, startcolumn, 4096);
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final InputStream dstream, final String encoding) throws UnsupportedEncodingException {
    this(dstream, encoding, 1, 1, 4096);
  }

  /**
   * Constructor.
   */
  public SimpleCharStream(final InputStream dstream) {
    this(dstream, 1, 1, 4096);
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final InputStream dstream, final String encoding, final int startline, final int startcolumn, final int buffersize)
      throws UnsupportedEncodingException {
    ReInit(encoding == null ? new InputStreamReader(dstream) : new InputStreamReader(dstream, encoding), startline, startcolumn, buffersize);
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final InputStream dstream, final int startline, final int startcolumn, final int buffersize) {
    ReInit(new InputStreamReader(dstream), startline, startcolumn, buffersize);
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final InputStream dstream, final String encoding) throws UnsupportedEncodingException {
    ReInit(dstream, encoding, 1, 1, 4096);
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final InputStream dstream) {
    ReInit(dstream, 1, 1, 4096);
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final InputStream dstream, final String encoding, final int startline, final int startcolumn) throws UnsupportedEncodingException {
    ReInit(dstream, encoding, startline, startcolumn, 4096);
  }

  /**
   * Reinitialise.
   */
  public void ReInit(final InputStream dstream, final int startline, final int startcolumn) {
    ReInit(dstream, startline, startcolumn, 4096);
  }

  /**
   * Get token literal value.
   */
  public String GetImage() {
    if (bufpos >= tokenBegin)
      return new String(buffer, tokenBegin, bufpos - tokenBegin + 1);
    else
      return new String(buffer, tokenBegin, bufsize - tokenBegin) + new String(buffer, 0, bufpos + 1);
  }

  /**
   * Get the suffix.
   */
  public char[] GetSuffix(final int len) {
    final char[] ret = new char[len];

    if ((bufpos + 1) >= len)
      System.arraycopy(buffer, bufpos - len + 1, ret, 0, len);
    else {
      System.arraycopy(buffer, bufsize - (len - bufpos - 1), ret, 0, len - bufpos - 1);
      System.arraycopy(buffer, 0, ret, len - bufpos - 1, bufpos + 1);
    }

    return ret;
  }

  /**
   * Reset buffer when finished.
   */
  public void Done() {
    buffer = null;
    bufline = null;
    bufcolumn = null;
  }

  /**
   * Method to adjust line and column numbers for the start of a token.
   */
  public void adjustBeginLineColumn(int newLine, final int newCol) {
    int start = tokenBegin;
    final int len;

    if (bufpos >= tokenBegin) {
      len = bufpos - tokenBegin + inBuf + 1;
    } else {
      len = bufsize - tokenBegin + bufpos + 1 + inBuf;
    }

    int i = 0, j = 0, k = 0;
    int nextColDiff = 0, columnDiff = 0;

    while (i < len && bufline[j = start % bufsize] == bufline[k = ++start % bufsize]) {
      bufline[j] = newLine;
      nextColDiff = columnDiff + bufcolumn[k] - bufcolumn[j];
      bufcolumn[j] = newCol + columnDiff;
      columnDiff = nextColDiff;
      i++;
    }

    if (i < len) {
      bufline[j] = newLine++;
      bufcolumn[j] = newCol + columnDiff;

      while (i++ < len) {
        if (bufline[j = start % bufsize] != bufline[++start % bufsize])
          bufline[j] = newLine++;
        else
          bufline[j] = newLine;
      }
    }

    line = bufline[j];
    column = bufcolumn[j];
  }

}
/* JavaCC - OriginalChecksum=fcfdfad2e33a4962f8ba87db453fcec3 (do not edit this line) */
