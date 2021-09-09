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

package com.orientechnologies.orient.core.storage.cluster.v0;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetFreeListPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetRecordsSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetSizePO;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 20.08.13
 */
public final class OPaginatedClusterStateV0 extends ODurablePage {
  private static final int RECORDS_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = RECORDS_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_LIST_OFFSET = SIZE_OFFSET + OLongSerializer.LONG_SIZE;

  public OPaginatedClusterStateV0(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void setSize(long size) {
    final long oldSize = getLongValue(SIZE_OFFSET);
    setLongValue(SIZE_OFFSET, size);

    addPageOperation(new PaginatedClusterStateV0SetSizePO(oldSize, size));
  }

  public long getSize() {
    return getLongValue(SIZE_OFFSET);
  }

  public void setRecordsSize(long recordsSize) {
    final long oldRecordsSize = getLongValue(RECORDS_SIZE_OFFSET);
    setLongValue(RECORDS_SIZE_OFFSET, recordsSize);

    addPageOperation(new PaginatedClusterStateV0SetRecordsSizePO(oldRecordsSize, recordsSize));
  }

  public long getRecordsSize() {
    return getLongValue(RECORDS_SIZE_OFFSET);
  }

  public void setFreeListPage(int index, long pageIndex) {
    final int pageOffset = FREE_LIST_OFFSET + index * OLongSerializer.LONG_SIZE;
    final int oldPageIndex = (int) getLongValue(pageOffset);

    setLongValue(pageOffset, pageIndex);

    addPageOperation(
        new PaginatedClusterStateV0SetFreeListPagePO(index, oldPageIndex, (int) pageIndex));
  }

  public long getFreeListPage(int index) {
    return getLongValue(FREE_LIST_OFFSET + index * OLongSerializer.LONG_SIZE);
  }
}
