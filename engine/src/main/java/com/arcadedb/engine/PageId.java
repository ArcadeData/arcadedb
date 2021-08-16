/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.engine;

/**
 * Immutable.
 */
public class PageId implements Comparable<PageId> {
  private int fileId;
  private int pageNumber;

  public PageId(final int fileId, final int pageNumber) {
    this.fileId = fileId;
    this.pageNumber = pageNumber;
  }

  public int getFileId() {
    return fileId;
  }

  public int getPageNumber() {
    return pageNumber;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final PageId pageId = (PageId) o;

    if (fileId != pageId.fileId)
      return false;
    return pageNumber == pageId.pageNumber;
  }

  @Override
  public int hashCode() {
    int result = fileId;
    result = 31 * result + pageNumber;
    return result;
  }

  @Override
  public String toString() {
    return "PageId(" + fileId + "/" + pageNumber + ")";
  }

  @Override
  public int compareTo(final PageId o) {
    if (o == this)
      return 0;

    if (!(o instanceof PageId))
      throw new IllegalArgumentException("cannot compare a page id with " + o.getClass());

    if (fileId > o.fileId)
      return 1;
    else if (fileId < o.fileId)
      return -1;

    if (pageNumber > o.pageNumber)
      return 1;
    else if (pageNumber < o.pageNumber)
      return -1;
    return 0;
  }
}
