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
package com.arcadedb.index.lsm;

import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;

import java.io.*;
import java.util.*;

public class LSMTreeIndexDebugger {
  public static void out(final int indent, final String text) {
    System.out.println(" ".repeat(indent) + text);
  }

  public static void printMutableIndex(final LSMTreeIndexAbstract index) {
    final int totalPages = index.getTotalPages();

    int lastImmutablePage = totalPages - 1;
    for (int pageIndex = totalPages - 1; pageIndex > -1; --pageIndex) {
      final BasePage page;
      try {
        page = index.getDatabase().getPageManager().getPage(new PageId(index.getFileId(), pageIndex), index.getPageSize(), false, true);
        if (!index.isMutable(page)) {
          lastImmutablePage = pageIndex;
          break;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    out(0, "MUTABLE INDEX " + index.getName() + " lastImmutablePage=" + lastImmutablePage + "/" + totalPages);
    for (int pageIndex = 0; pageIndex < totalPages; ++pageIndex) {
      final BasePage page;
      try {
        page = index.getDatabase().getPageManager().getPage(new PageId(index.getFileId(), pageIndex), index.getPageSize(), false, true);
        LSMTreeIndexDebugger.out(1, LSMTreeIndexDebugger.printMutableIndexPage(index, page));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static String printMutableIndexPage(final LSMTreeIndexAbstract index, final BasePage page) {
    String buffer = "";
    buffer +=
        "MUTABLE INDEX - PAGE " + page.getPageId() + " mutable=" + index.isMutable(page) + " size=" + page.getPhysicalSize() + " v" + page.getVersion() + " "
            + Arrays.toString(index.getKeyTypes());
    final Object[] pageKeyRange = index.getPageKeyRange(page);
    buffer += " Keys: " + index.getCount(page) + Arrays.toString((Object[]) pageKeyRange[0]) + "-" + Arrays.toString((Object[]) pageKeyRange[1]) + " - header: "
        + index.getHeaderSize(page.getPageId().getPageNumber()) + " - valuesFreePosition: " + index.getValuesFreePosition(page);
    return buffer;
  }
}
