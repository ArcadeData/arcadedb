/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.cluster.v2;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetFileSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetFreeListPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetRecordsSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetSizePO;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 20.08.13
 */
public final class OPaginatedClusterStateV2 extends ODurablePage {
  private static final int RECORDS_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = RECORDS_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FILE_SIZE_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FREE_LIST_OFFSET = FILE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  public OPaginatedClusterStateV2(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void setSize(int size) {
    final int oldSize = getIntValue(SIZE_OFFSET);

    setIntValue(SIZE_OFFSET, size);
    addPageOperation(new PaginatedClusterStateV2SetSizePO(oldSize, size));
  }

  public int getSize() {
    return getIntValue(SIZE_OFFSET);
  }

  public void setRecordsSize(int recordsSize) {
    final int oldRecordsSize = getIntValue(RECORDS_SIZE_OFFSET);

    setIntValue(RECORDS_SIZE_OFFSET, recordsSize);
    addPageOperation(new PaginatedClusterStateV2SetRecordsSizePO(oldRecordsSize, recordsSize));
  }

  public int getRecordsSize() {
    return getIntValue(RECORDS_SIZE_OFFSET);
  }

  public void setFileSize(int size) {
    final int oldFileSize = getIntValue(FILE_SIZE_OFFSET);

    setIntValue(FILE_SIZE_OFFSET, size);
    addPageOperation(new PaginatedClusterStateV2SetFileSizePO(oldFileSize, size));
  }

  public int getFileSize() {
    return getIntValue(FILE_SIZE_OFFSET);
  }

  public void setFreeListPage(int index, int pageIndex) {
    final int pageOffset = FREE_LIST_OFFSET + index * OIntegerSerializer.INT_SIZE;
    final int oldPageIndex = getIntValue(pageOffset);

    setIntValue(pageOffset, pageIndex);

    addPageOperation(new PaginatedClusterStateV2SetFreeListPagePO(index, oldPageIndex, pageIndex));
  }

  public int getFreeListPage(int index) {
    return getIntValue(FREE_LIST_OFFSET + index * OIntegerSerializer.INT_SIZE);
  }
}
