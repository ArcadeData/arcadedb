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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extends a FileComponent by supporting pages.
 * <p>
 * HEADER = [recordCount(int:4)] CONTENT-PAGES = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(512*ushort=2048)]
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class PaginatedComponent extends Component {
  public static final String                 TEMP_EXT  = "temp_";
  protected final     PaginatedComponentFile file;
  protected final     int                    pageSize;
  protected final     AtomicInteger          pageCount = new AtomicInteger();
  /**
   * Atomic counter used by allocators (e.g., {@link LocalBucket}) to reserve unique page numbers
   * across concurrent transactions <b>before</b> a tx commits. Unlike {@code pageCount}, which
   * reflects committed pages and is exposed via {@link #getTotalPages()}, this counter advances
   * eagerly at allocation time and is invisible to other transactions doing space lookups.
   * <p>
   * Without this separation, two concurrent transactions could both compute the same "next page
   * number" and end up writing different chunks to the same physical page slot &mdash; a silent
   * data-corruption bug that the MVCC version check did not catch reliably.
   */
  protected final     AtomicInteger          reservedPageCounter = new AtomicInteger();

  protected PaginatedComponent(final DatabaseInternal database, final String name, final String filePath, final String ext,
      final ComponentFile.MODE mode,
      final int pageSize, final int version) throws IOException {
    this(database, name, filePath, ext, database.getFileManager().newFileId(), mode, pageSize, version);
  }

  private PaginatedComponent(final DatabaseInternal database, final String name, final String filePath, final String ext,
      final int id,
      final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    this(database, name, filePath + "." + id + "." + pageSize + ".v" + version + "." + ext, id, mode, pageSize, version);
  }

  protected PaginatedComponent(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode,
      final int pageSize, final int version) throws IOException {
    super(database, name, id, version, filePath);
    if (pageSize <= 0)
      throw new IllegalArgumentException("Invalid page size " + pageSize);

    this.pageSize = pageSize;
    this.file = (PaginatedComponentFile) database.getFileManager().getOrCreateFile(name, filePath, mode);

    // The component and the file it holds MUST agree on the id (issue #6283). getOrCreateFile() is keyed by the
    // component NAME and not by the path, so a name that is already registered hands back that file - carrying
    // whatever id it was created with - while this component keeps the id it was built with, which on the
    // id-allocating path above is a brand new one from FileManager.newFileId(). The component would then address
    // pages of a file id that is not the file it holds. In #6198 that surfaced as "File with id 2 was not found"
    // thrown from PageManager.loadPage, far from the cause; the variant where the bogus id happens to resolve to
    // some other component's file is considerably worse than an exception, hence the loud failure right here,
    // before a single page is addressed.
    // A caller that legitimately has to build on an already-registered file resolves it FIRST through
    // FileManager.getFileByComponentName() and constructs on that file's real id, as
    // TimeSeriesTagDictionary.openOrCreate() does.
    // The id newFileId() reserved for a construction that ends here is abandoned: nothing fills that slot and
    // nothing reclaims it. Deliberate - this is a programming error on a path that no legitimate caller takes,
    // and one leaked slot in the file table costs far less than a reclaim protocol racing concurrent creations.
    if (file.getFileId() != fileId)
      throw new IllegalStateException(
          "Component '" + name + "' was built on file id " + fileId + " but the file registered under that name has id "
              + file.getFileId() + " ('" + file.getFilePath() + "')");

    // The same invariant as the id one, on the other field a component uses to address its file (issue #6314): the
    // page size IS the stride, so a component that disagrees with its file reads page N from N * theWrongNumber.
    // PageManager resolves every page with the CALLER's page size, and only the snapshot path ever consults the
    // file's own, so nothing downstream turns that into an exception - it is real bytes at the wrong offset, and
    // pageCount below is computed from the wrong divisor on top. The page size is baked into the file name, so this
    // can only be a component that re-derived it from somewhere else (a live configuration value, say) instead of
    // taking the one ComponentFactory parsed off the name.
    if (file.getPageSize() != pageSize)
      throw new IllegalStateException(
          "Component '" + name + "' was built with page size " + pageSize + " but its file '" + file.getFilePath()
              + "' has page size " + file.getPageSize());

    // The third and last fact baked into a component file's name - 'name.fileId.pageSize.vVersion.ext' - and so the
    // third guard of the same set (issue #6340). It decides how the pages are INTERPRETED where the other two decide
    // where they are: a LocalBucket version selects the record-header layout, a TimeSeriesBucket version selects
    // whether a TAG column is a 4-byte dictionary id or an inline string, and reading one as the other is a
    // misinterpretation of real bytes rather than an exception - the same failure shape as the page size.
    //
    // THIS IS A TRIPWIRE AND NOT A COMPATIBILITY GATE, and the difference is worth stating because #6314 had to
    // work through it twice: every load constructor passes the version ComponentFactory parsed OFF THE FILE NAME
    // straight through, and every creation path bakes the version it was given into the name it generates, so a
    // component and its file agree by construction whatever build wrote the file. An old database therefore cannot
    // trip this. What can is a component that re-derived its version from somewhere other than its file - claiming
    // CURRENT_VERSION for a file whose name says otherwise, which is exactly the defect #6314 removed from the two
    // TimeSeries factory handlers and exactly what nobody should reintroduce.
    if (file.getVersion() != version)
      throw new IllegalStateException(
          "Component '" + name + "' was built with version " + version + " but its file '" + file.getFilePath()
              + "' has version " + file.getVersion());

    // `fileSize == 0` is not a special case: 0 / pageSize is already 0, so the division below covers it too.
    final long fileSize = file.getSize();
    pageCount.set((int) (fileSize / pageSize));

    // A file killed part way through writing a page leaves a tail shorter than one whole page, which the division
    // above silently floors away: nothing counts it, nothing repairs it. Unlike the file-id (#6283) and page-size
    // (#6314) guards above, this is NOT a tripwire, because the two thrown cases are programming errors that
    // cannot exist in a file written by a correct build, while a torn tail is exactly what a power cut produces -
    // refusing to open on it would turn an ordinary crash into a database that a previous build opened happily
    // into one this one won't. It is also not silent content loss for a bucket or index: the next page appended
    // to this file lands at `pageCount * pageSize` (see PaginatedComponentFile.write()), overwriting the stray
    // bytes wholesale rather than reading them, so the only cost is the wasted bytes on disk until that happens.
    final long tailBytes = fileSize % pageSize;
    if (tailBytes != 0)
      LogManager.instance().log(this, Level.WARNING,
          "Component '%s' file '%s' has a length (%d bytes) that is not a multiple of its page size (%d bytes): "
              + "%d trailing bytes past the last complete page (page count %d) are not a complete page and were ignored. This is consistent with "
              + "the process being killed while writing that page; it will be overwritten the next time a page is "
              + "appended to this file",
          null, name, file.getFilePath(), fileSize, pageSize, tailBytes, pageCount.get());

    reservedPageCounter.set(pageCount.get());
  }

  public void rename(final String newComponentName) throws IOException {
    // #4928: the bounded wait can give up on a wedged flush. Renaming with pages still in flight would let
    // the flush thread write to the file under its old identity: abort loudly instead, the rename can be
    // retried once the flush recovers.
    if (!PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(database))
      throw new IOException("Cannot rename component '" + componentName
          + "': pages are still pending flush after the no-progress timeout (see arcadedb.flushAllPagesTimeout)");
    // The argument is the new component name, not a type name and not a file name: the file layer recomposes the
    // '.fileId.pageSize.vVersion.ext' tail itself, so nothing here has to locate where the name ends.
    file.renameComponent(newComponentName);

    database.getFileManager().renameFile(componentName, newComponentName);
    componentName = newComponentName;
  }

  public PaginatedComponentFile getComponentFile() {
    return file;
  }

  public File getOSFile() {
    return file.getOSFile();
  }

  public int getPageSize() {
    return pageSize;
  }

  public void updatePageCount(final int totalPages) {
    // USE IF TO SPEED UP THE CHECK
    if (totalPages > pageCount.get())
      pageCount.updateAndGet(current -> Math.max(totalPages, current));
    if (totalPages > reservedPageCounter.get())
      reservedPageCounter.updateAndGet(current -> Math.max(totalPages, current));
  }

  @Override
  public void close() {
    if (file != null)
      file.close();
  }

  public int getTotalPages() {
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null) {
      final Integer txPageCounter = tx.getPageCounter(fileId);
      if (txPageCounter != null)
        return txPageCounter;
    }
    return pageCount.get();
  }

  public void removeTempSuffix() {
    final String fileName = file.getFilePath();

    final int extPos = fileName.lastIndexOf('.');
    if (fileName.substring(extPos + 1).startsWith(TEMP_EXT)) {
      final String newFileName = fileName.substring(0, extPos) + "." + fileName.substring(extPos + TEMP_EXT.length() + 1);

      LogManager.instance().log(this, Level.FINE,
          "removeTempSuffix: fileId=%d componentName='%s' renaming '%s' -> '%s'",
          null, fileId, componentName, fileName, newFileName);

      try {
        file.rename(newFileName);
        database.getFileManager().renameFile(fileName, newFileName);
        // Sync the active FileManager recording session with the new on-disk file name so any
        // FileChange entry captured at file creation reflects the post-rename name (#4083).
        database.getFileManager().refreshRecordedFileName(file);
        LogManager.instance().log(this, Level.FINE,
            "removeTempSuffix completed: fileId=%d componentName='%s' postRenameFileName='%s'",
            null, fileId, componentName, file.getFileName());
      } catch (final IOException e) {
        throw new IndexException(
            "Cannot rename temporary index file '" + file.getFilePath() + "' to '" + newFileName + "' (exists=" + (new File(
                file.getFilePath()).exists()) + ")", e);
      }
    }
  }
}
