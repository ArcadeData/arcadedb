# Comprehensive Analysis of ArcadeDB LSM-Tree Index Implementation

## 1. CLASS HIERARCHY AND INHERITANCE STRUCTURE

### 1.1 Inheritance Chain

```
PaginatedComponent (abstract base)
    └── LSMTreeIndexAbstract (abstract)
            ├── LSMTreeIndexMutable (mutable level-0 index)
            └── LSMTreeIndexCompacted (immutable compacted index)

LSMTreeIndex (facade/wrapper)
    └── Contains: LSMTreeIndexMutable (mutable)
                  └── References: LSMTreeIndexCompacted (immutable sub-index)
```

### 1.2 Class Responsibilities

**PaginatedComponent** (`/engine/src/main/java/com/arcadedb/engine/PaginatedComponent.java`)
- Base class for all page-based components
- Manages page count and file associations
- Handles transaction-aware page counting
- Provides basic file I/O operations

**LSMTreeIndexAbstract** (`LSMTreeIndexAbstract.java`)
- **Role**: Core abstract foundation for LSM index operations
- **Key Responsibilities**:
  - Page layout management (header and content regions)
  - Key-value serialization/deserialization
  - Binary comparison operations
  - Lookup algorithms (dichotomy/binary search)
  - Transaction integration hooks
  - NULL_STRATEGY enforcement
  - Cursor creation and management
  - Page traversal (forward/backward)

**LSMTreeIndexMutable** (`LSMTreeIndexMutable.java`)
- **Role**: Level-0 (mutable) index for recent writes
- **Key Responsibilities**:
  - Accepts write operations (put/remove)
  - Manages active pages (writable)
  - Triggers compaction when page threshold is reached
  - Maintains reference to sub-index (LSMTreeIndexCompacted)
  - Creates new pages on full conditions
  - Handles multi-value entry splitting across pages

**LSMTreeIndexCompacted** (`LSMTreeIndexCompacted.java`)
- **Role**: Level-1+ (immutable) compacted index
- **Key Responsibilities**:
  - Read-only operations
  - Append-only writes during compaction
  - Page series management for root indexing
  - Search optimization using compacted page series structure

**LSMTreeIndex** (`LSMTreeIndex.java`)
- **Role**: Facade and lifecycle manager
- **Key Responsibilities**:
  - Public API implementation (RangeIndex interface)
  - Transaction integration
  - Status management (AVAILABLE, COMPACTING, etc.)
  - File replacement during compaction
  - Build/rebuild operations
  - Concurrent access via RWLockContext

### 1.3 Key Design Patterns

1. **Facade Pattern**: LSMTreeIndex wraps LSMTreeIndexMutable/Compacted
2. **Template Method Pattern**: LSMTreeIndexAbstract defines abstract methods (compareKey) overridden by subclasses
3. **Iterator/Cursor Pattern**: Multiple cursor types for traversal
4. **Two-Level LSM**: Mutable + Compacted index levels

---

## 2. PAGE-BASED STORAGE ARCHITECTURE

### 2.1 Page Layout

**Header Structure (1st Page = Root Page)**:
```
OFFSET(bytes)    SIZE      FIELD                              PURPOSE
0-3              4         offsetFreeKeyValueContent (int)   Points to free space for key/value pairs
4-7              4         numberOfEntries (int)              Count of keys in page
8                1         mutable (boolean)                  Is page writable?
9-12             4         compactedPageNumberOfSeries (int) Used by compacted index for series tracking
13-16            4         subIndexFileId (int)               File ID of compacted sub-index (only page 0)
17               1         numberOfKeys (byte)                Key count in composite key (only page 0)
18+              N         keyType(byte)[numberOfKeys]        Binary type codes for each key (only page 0)
```

**Header Structure (Non-Root Pages)**:
```
OFFSET(bytes)    SIZE      FIELD
0-3              4         offsetFreeKeyValueContent
4-7              4         numberOfEntries
8                1         mutable
9-12             4         compactedPageNumberOfSeries
```

**Page Content Layout**:
```
┌─────────────────────────────────────────────┐
│ HEADER (fixed size based on page number)   │
├─────────────────────────────────────────────┤
│ INDEX ARRAY (int pointers to key/values)   │  ← grows downward
│ [pointer0][pointer1]...[pointerN]          │
├─────────────────────────────────────────────┤
│                                             │  ← free space
├─────────────────────────────────────────────┤
│ KEY/VALUE PAIRS (stored from tail)         │  ← grows upward
│ ...[keyN|valueN][key1|value1][key0|value0]│
└─────────────────────────────────────────────┘

When page is full: head (pointers) meets tail (key/values)
```

### 2.2 Key-Value Serialization

**Key Serialization Format**:
```
Per Key in Composite Key:
[notNullByte(1)] [value(variable)]
  - notNullByte: 0 = NULL, 1 = present
  - value: Serialized using BinarySerializer based on Type
```

**Value Serialization Format**:
```
[numberOfValues(int)] [rid1(compressed)] [rid2(compressed)] ... [ridN(compressed)]
  - numberOfValues: Count of RID references (for non-unique indexes)
  - Each RID: Compressed using BinaryTypes.TYPE_COMPRESSED_RID
```

**Deleted Entry Marker**:
```
When an entry is logically deleted:
  RID.bucketId = negative value (< 0)
  RID.position = original position
  Encoding: bucketId = (original_bucketId + 2) * -1
  Original recovered: bucketId = (abs(bucketId) - 2)
```

### 2.3 Page Lifecycle

1. **Creation**: `createNewPage()` allocates new MutablePage
   - Initial free position = page.getMaxContentSize()
   - Entry count = 0
   - Mutable flag = true

2. **Growth**: As entries added, free position decreases (grows from tail)
   - Index array grows down from header
   - Key/value pairs grow up from tail
   - When meeting point reached → page full → create new page

3. **Immutability**: When page full, setMutable(page, false)
   - Page becomes read-only
   - Compaction can process immutable pages

4. **Compaction**: Immutable pages merged into compacted index
   - Multiple immutable pages merged by key
   - Duplicate keys consolidated
   - New compacted page structure created

### 2.4 Page Access Patterns

**Mutable Index Operations**:
```java
// Read access (immutable)
BasePage currentPage = database.getTransaction().getPage(pageId, pageSize);

// Write access (mutable)
MutablePage currentPage = database.getTransaction()
    .getPageToModify(pageId, pageSize, isNew);

// Page creation
MutablePage newPage = database.isTransactionActive() ?
    database.getTransaction().addPage(pageId, pageSize) :
    new MutablePage(pageId, pageSize);
```

**Compacted Index Operations**:
```java
// Compacted uses new MutablePage directly (during compaction)
MutablePage newPage = new MutablePage(pageId, pageSize);

// Series tracking:
// compactedPageNumberOfSeries = number of pages in current series
// Used to build root page index for B-tree-like search
```

---

## 3. TRANSACTION SYSTEM INTEGRATION

### 3.1 Transaction-Aware Operations

**Put Operation Flow**:
```
LSMTreeIndex.put(keys, rids)
    ↓
Checks transaction status via database.getTransaction().getStatus()
    ↓
IF TRANSACTION ACTIVE:
    → Add to transaction index changes (deferred)
    → Operation applied at COMMIT time (in write lock)
ELSE:
    → Execute immediately in read lock
    → Direct call to mutable.put()
```

**Key Transaction Integration Points**:
1. **Deferred Writes**: `transaction.addIndexOperation(IndexKeyOperation.ADD)`
2. **Transaction Context Access**: `database.getTransaction()`
3. **Page Modification Tracking**: Pages are tracked within transaction
4. **Commit-time Application**: Changes applied in write lock

### 3.2 Transaction Index Context

**ComparableKey** (TransactionIndexContext.ComparableKey):
- Wraps Object[] keys for comparison in TreeMap
- Enables sorted ordering of pending changes
- Used for deduplication across cursors

**IndexKey** (TransactionIndexContext.IndexKey):
- Represents pending index operation
- Fields: operation (ADD/REMOVE), rid, key
- Stored in nested maps: ComparableKey → IndexKey → IndexKey (map for dedup)

**Removed Entry Tracking**:
```java
// For each cursor/page, track removed keys:
Set<TransactionIndexContext.ComparableKey> removedKeys

// When iterating:
if (removedKeys.contains(key))
    // Skip this entry (already deleted in transaction)
```

### 3.3 Page Management in Transactions

**Page Acquisition**:
```java
// Get immutable copy (for reads)
BasePage page = tx.getPage(pageId, pageSize);

// Get mutable copy (for writes)
MutablePage page = tx.getPageToModify(pageId, pageSize, isNew);
  - isNew: true if page being created
  - Marks page as modified in transaction

// Create new page
MutablePage newPage = tx.addPage(pageId, pageSize);
  - Increments transaction page counter
  - Tracked for WAL and flush
```

**Page Version Management**:
```java
// After modifications, update version
MutablePage modified = database.getPageManager()
    .updatePageVersion(mutablePage, dirty);
  - dirty: true for modified pages
  - Version incremented for concurrency control
```

**Page Flushing**:
```java
// During commit
database.getPageManager().writePages(modifiedPages, asyncFlush);
  - asyncFlush: true for background flush
  - false during compaction (synchronous)
  - Updates Write-Ahead Log (WAL)
```

### 3.4 Transaction State Machine

```
INDEX_STATUS states:
- AVAILABLE: Normal operation (accepts reads/writes)
- COMPACTION_SCHEDULED: Compaction queued (still accepts ops)
- COMPACTION_IN_PROGRESS: Compacting (delayed new ops)
- UNAVAILABLE: Closing (rejects operations)

Transitions:
AVAILABLE → COMPACTION_SCHEDULED (via scheduleCompaction)
    ↓
COMPACTION_SCHEDULED → COMPACTION_IN_PROGRESS (via compact)
    ↓
COMPACTION_IN_PROGRESS → AVAILABLE (on success)
    ↓
AVAILABLE → UNAVAILABLE (via close)
```

---

## 4. COMPONENT FACTORY PATTERN

### 4.1 Index Creation Path

**During Index Definition**:
```
IndexBuilder.build()
    ↓
IndexFactory.createIndex(builder)
    ↓
IndexFactory.register("LSM_TREE", LSMTreeIndex.IndexFactoryHandler)
    ↓
handler.create(builder)
    ↓
return new LSMTreeIndex(database, name, unique, filePath, mode,
                        keyTypes, pageSize, nullStrategy)
```

**Factory Handler Pattern**:
```java
// LSMTreeIndex.java - inner static class
public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
        return new LSMTreeIndex(
            builder.getDatabase(),
            builder.getIndexName(),
            builder.isUnique(),
            builder.getFilePath(),
            ComponentFile.MODE.READ_WRITE,
            builder.getKeyTypes(),
            builder.getPageSize(),
            builder.getNullStrategy()
        );
    }
}
```

**Registration at Startup** (LocalSchema.java):
```java
indexFactory.register(INDEX_TYPE.LSM_TREE.name(),
                     new LSMTreeIndex.IndexFactoryHandler());
indexFactory.register(INDEX_TYPE.FULL_TEXT.name(),
                     new LSMTreeFullTextIndex.IndexFactoryHandler());
indexFactory.register(INDEX_TYPE.HNSW.name(),
                     new HnswVectorIndex.IndexFactoryHandler());

// PaginatedComponent factories
componentFactory.registerComponent("umtidx",  // unique mutable
                     new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
componentFactory.registerComponent("numtidx", // non-unique mutable
                     new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());
componentFactory.registerComponent("uctidx",  // unique compacted
                     new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
componentFactory.registerComponent("nuctidx", // non-unique compacted
                     new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());
```

### 4.2 Index Loading Path

**On Database Restart**:
```
Database.load()
    ↓
For each index file:
    ComponentFactory.createComponent(file, mode)
        ↓
    Lookup handler by file extension (umtidx, numtidx, uctidx, nuctidx)
        ↓
    handler.createOnLoad(database, name, filePath, fileId, mode,
                         pageSize, version)
        ↓
    If file ends with COMPACTED_EXT:
        return new LSMTreeIndexCompacted(...)
    Else:
        return new LSMTreeIndex(...).mutable
```

**LSMTreeIndex.PaginatedComponentFactoryHandlerUnique**:
```java
public static class PaginatedComponentFactoryHandlerUnique
    implements ComponentFactory.PaginatedComponentFactoryHandler {

    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database,
                                          final String name,
                                          final String filePath,
                                          final int id,
                                          final ComponentFile.MODE mode,
                                          final int pageSize,
                                          final int version) throws IOException {
        if (filePath.endsWith(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT))
            return new LSMTreeIndexCompacted(null, database, name, true,
                                            filePath, id, mode, pageSize, version);

        return new LSMTreeIndex(database, name, true, filePath, id,
                               mode, pageSize, version).mutable;
    }
}
```

### 4.3 Key Design Insights

1. **Two Factories**:
   - **IndexFactory**: Creates logical LSMTreeIndex wrapper
   - **ComponentFactory**: Creates physical PaginatedComponent (Mutable/Compacted)

2. **File Extension Routing**:
   - Extension determines handler: umtidx, numtidx, uctidx, nuctidx
   - umtidx/uctidx: unique flag set to true
   - numtidx/nuctidx: unique flag set to false
   - "ctidx" suffix indicates compacted index

3. **Lazy Loading**:
   - Only 1st page loaded on startup (via constructor that takes id)
   - Full index metadata loaded on demand via onAfterLoad()
   - Sub-index loaded from root page field: subIndexFileId

4. **Unique vs Non-Unique**:
   - Same code path, distinguished only by `unique` boolean flag
   - Different file extensions allow loading with correct uniqueness

---

## 5. CONCURRENT ACCESS AND TRANSACTION ISOLATION

### 5.1 Lock Architecture

**RWLockContext** (used by LSMTreeIndex):
```java
private final RWLockContext lock = new RWLockContext();

// Read lock for GET operations
lock.executeInReadLock(() -> mutable.get(keys, limit));

// Write lock for PUT/REMOVE operations
lock.executeInWriteLock(() -> {
    mutable.put(keys, rids);
    return null;
});

// Index compaction also uses write lock
lock.executeInWriteLock(() -> {
    // Replace mutable index with new one after compaction
    mutable = newMutableIndex;
    return newMutableIndex;
});
```

### 5.2 Isolation Strategies

**Read Isolation (GET)**:
```java
public IndexCursor get(final Object[] keys, final int limit) {
    // 1. Check for transaction changes (highest priority)
    if (getDatabase().getTransaction().getStatus() == BEGUN) {
        Map<ComparableKey, ...> indexChanges =
            tx.getIndexChanges().getIndexKeys(getName());
        // Build txCursor from changes
    }

    // 2. Read from index under read lock
    IndexCursor result = lock.executeInReadLock(
        () -> mutable.get(convertedKeys, limit)
    );

    // 3. Merge transaction changes with index results
    if (txChanges != null) {
        return new TempIndexCursor(merged);
    }
    return result;
}
```

**Write Isolation (PUT)**:
```java
public void put(final Object[] keys, final RID[] rids) {
    if (getDatabase().getTransaction().getStatus() == BEGUN) {
        // Defer to transaction
        tx.addIndexOperation(this, ADD, convertedKeys, rid);
    } else {
        // Immediate write under read lock (!)
        lock.executeInReadLock(() -> {
            mutable.put(convertedKeys, rids);
            return null;
        });
    }
}
```

**Removed Entry Tracking**:
```java
// During iteration, track removed keys per cursor
Set<TransactionIndexContext.ComparableKey> removedKeys = new HashSet<>();

for (RID rid : allValues) {
    if (rid.getBucketId() < 0) {
        // This is a removal marker
        removedKeys.add(keys);
        continue;
    }

    if (removedKeys.contains(keys))
        // Skip - already deleted
        continue;

    validRIDs.add(rid);
}
```

### 5.3 Multi-Cursor Coordination

**LSMTreeIndexCursor** (main cursor):
```
Multiple underlying cursors (one per mutable page + compacted series)
    ↓
Ordered by page number (newest mutable first, then compacted)
    ↓
Merge-sorted iteration over all cursors
    ↓
Deduplication at each key level
    ↓
Removal marker handling (last RID is removal if negative)
```

**Cursor Initialization**:
```java
// From LSMTreeIndexCursor constructor
for (int pageId = totalPages - 1; pageId > -1; --pageId) {
    // Create cursor for each page (newest first)
    pageCursors[cursorIdx] = index.newPageIterator(pageId, ...);
}

// Then add compacted cursors (oldest)
for (compactedCursor : compactedSeriesIterators) {
    pageCursors[totalPages + i] = compactedCursor;
}
```

**Merge-Sort Next()**:
```java
public RID next() {
    while (hasNext()) {
        // 1. Find minimum key across all active cursors
        Object[] minorKey = null;
        List<Integer> minorKeyIndexes = new ArrayList<>();
        for (cursor : pageCursors) {
            if (cursor != null) {
                int cmp = compareKeys(cursor.getKeys(), minorKey);
                if (minorKey == null || cmp < 0) {
                    minorKey = cursor.getKeys();
                    minorKeyIndexes.clear();
                }
                if (cmp == 0) minorKeyIndexes.add(idx);
            }
        }

        // 2. Merge values from all cursors with same key
        Set<RID> validRIDs = new HashSet<>();
        for (int idx : minorKeyIndexes) {
            RID[] values = pageCursors[idx].getValue();
            // Filter removed entries
            for (RID rid : values) {
                if (rid.getBucketId() >= 0)
                    validRIDs.add(rid);
            }
            // Advance cursor
            pageCursors[idx].next();
        }

        // 3. Return first valid RID
        if (!validRIDs.isEmpty())
            return validRIDs.iterator().next();
    }
}
```

### 5.4 Compaction and Consistency

**During Compaction**:
```
1. Compaction scheduled: status = COMPACTION_SCHEDULED
   → New PUT/REMOVE operations still accepted
   → Queued in new pages of mutable index

2. Compaction in progress: status = COMPACTION_IN_PROGRESS
   → GET operations proceed normally (read existing pages)
   → Reads see both old and compacted data
   → PUT/REMOVE still queued

3. Index swap (write lock):
   → All operations momentarily blocked
   → mutable reference updated
   → File ID mapping updated in schema
   → Back to AVAILABLE

4. Old mutable file dropped
```

---

## 6. BUILD() AND PUT() METHODS

### 6.1 The build() Method

**Purpose**: Rebuild index from existing data (or initial population)

**Code Flow** (LSMTreeIndex.java lines 626-659):
```java
public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    checkIsValid();
    final AtomicLong total = new AtomicLong();

    if (propertyNames == null || propertyNames.isEmpty())
        throw new IndexException("Cannot rebuild index '" + name
            + "' because metadata information are missing");

    final DatabaseInternal db = getDatabase();

    // Mark index as building
    if (!status.compareAndSet(AVAILABLE, UNAVAILABLE))
        throw new NeedRetryException("Error on building index '" + name
            + "' because not available");

    try {
        // Scan bucket and add each record to index
        db.scanBucket(db.getSchema().getBucketById(
            associatedBucketId).getName(), record -> {

            // Add record to index
            db.getIndexer().addToIndex(LSMTreeIndex.this,
                record.getIdentity(), (Document) record);
            total.incrementAndGet();

            // Batch commit
            if (total.get() % buildIndexBatchSize == 0) {
                db.getWrappedDatabaseInstance().commit();
                db.getWrappedDatabaseInstance().begin();
            }

            // User callback
            if (callback != null)
                callback.onDocumentIndexed((Document) record, total.get());

            return true;
        });

        status.set(AVAILABLE);
    } else
        throw new NeedRetryException("Error on building index '" + name
            + "' because not available");

    return total.get();
}
```

**Key Characteristics**:
1. **Batch Processing**: Commits every buildIndexBatchSize records
2. **Transaction Wrapped**: Each batch in its own transaction
3. **Callback Support**: Allows UI updates during build
4. **Status Tracking**: UNAVAILABLE during build, AVAILABLE after
5. **Bucket Scanning**: Sequential scan of associated bucket
6. **External Indexer**: Uses database.getIndexer().addToIndex()

### 6.2 The put() Method

**Two-Level Put (LSMTreeIndex.java lines 431-445)**:

Level 1: LSMTreeIndex (facade):
```java
public void put(final Object[] keys, final RID[] rids) {
    checkIsValid();
    final Object[] convertedKeys = convertKeys(keys);

    if (getDatabase().getTransaction().getStatus() == BEGUN) {
        // Defer to transaction commit
        final TransactionContext tx = getDatabase().getTransaction();
        for (final RID rid : rids)
            tx.addIndexOperation(this,
                TransactionIndexContext.IndexKey.IndexKeyOperation.ADD,
                convertedKeys, rid);
    } else {
        // Immediate write
        lock.executeInReadLock(() -> {
            mutable.put(convertedKeys, rids);
            return null;
        });
    }
}
```

Level 2: LSMTreeIndexMutable (actual implementation):
```java
protected void internalPut(final Object[] keys, final RID[] rids) {
    // Validate
    if (keys == null)
        throw new IllegalArgumentException("Keys parameter is null");
    if (rids == null)
        throw new IllegalArgumentException("RIDs is null");
    if (keys.length != binaryKeyTypes.length)
        throw new IllegalArgumentException("Cannot put with partial key");

    checkForNulls(keys);  // Check NULL_STRATEGY

    // Skip if all keys are null
    final Object[] convertedKeys = convertKeys(keys, binaryKeyTypes);
    if (convertedKeys == null && nullStrategy == NULL_STRATEGY.SKIP)
        return;

    // Must be in transaction
    database.checkTransactionIsActive(database.isAutoTransaction());

    // Get current page (last mutable page)
    int pageNum = getTotalPages() - 1;
    MutablePage currentPage = database.getTransaction()
        .getPageToModify(new PageId(database, file.getFileId(), pageNum),
                        pageSize, false);

    TrackableBinary currentPageBuffer = currentPage.getTrackable();
    int count = getCount(currentPage);

    // Lookup where to insert
    final LookupResult result = lookupInPage(pageNum, count,
        currentPageBuffer, convertedKeys, 1);  // purpose=1 (retrieve)

    int keyValueFreePosition = getValuesFreePosition(currentPage);
    int keyIndex = result.found ? result.keyIndex + 1 : result.keyIndex;

    final Binary keyValueContent = database.getContext().getTemporaryBuffer1();

    int freeSpaceInPage = keyValueFreePosition -
        (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE));

    // Serialize key/value pairs
    RID[] values = rids;
    do {
        // Try to fit all values in current space
        int writtenValues = freeSpaceInPage - INT_SERIALIZED_SIZE > 0 ?
            writeEntryMultipleValues(keyValueContent, convertedKeys, values,
                freeSpaceInPage - INT_SERIALIZED_SIZE,
                currentPage.getMaxContentSize() - getHeaderSize(pageNum),
                currentPage.getPageId())
            : 0;

        boolean newPage = false;
        final boolean mutablePage = isMutable(currentPage);

        // Check if need new page
        if (!mutablePage || writtenValues == 0) {
            if (mutablePage)
                setMutable(currentPage, false);  // Mark old page immutable

            // Create new page
            newPage = true;
            currentPage = createNewPage();
            currentPageBuffer = currentPage.getTrackable();
            pageNum = currentPage.getPageId().getPageNumber();
            count = 0;
            keyIndex = 0;
            keyValueFreePosition = currentPage.getMaxContentSize();

            // Re-serialize to new page
            freeSpaceInPage = keyValueFreePosition -
                (getHeaderSize(pageNum) + INT_SERIALIZED_SIZE);
            writtenValues = writeEntryMultipleValues(keyValueContent,
                convertedKeys, values, freeSpaceInPage,
                currentPage.getMaxContentSize() - getHeaderSize(pageNum),
                currentPage.getPageId());
        }

        // Update free position
        keyValueFreePosition -= keyValueContent.size();

        // Write key/value content to page
        currentPageBuffer.putByteArray(keyValueFreePosition,
            keyValueContent.toByteArray());

        // Update index array (shift pointers if necessary)
        final int startPos = getHeaderSize(pageNum) +
            (keyIndex * INT_SERIALIZED_SIZE);
        if (keyIndex < count)
            currentPageBuffer.move(startPos, startPos + INT_SERIALIZED_SIZE,
                (count - keyIndex) * INT_SERIALIZED_SIZE);

        // Add pointer to key/value in index array
        currentPageBuffer.putInt(startPos, keyValueFreePosition);

        // Update page metadata
        setCount(currentPage, count + 1);
        setValuesFreePosition(currentPage, keyValueFreePosition);

        // Log if debug enabled
        if (LogManager.instance().isDebugEnabled())
            LogManager.instance().log(this, Level.FINE,
                "Put entry %s=%s in index '%s' (page=%s countInPage=%d newPage=%s)",
                Arrays.toString(keys), Arrays.toString(rids),
                componentName, currentPage.getPageId(), count + 1, newPage);

        // If not all values written, continue with next page
        if (writtenValues < values.length) {
            values = Arrays.copyOfRange(values, writtenValues, values.length);
            setMutable(currentPage, false);
            newPage = true;
            currentPage = createNewPage();
            currentPageBuffer = currentPage.getTrackable();
            pageNum = currentPage.getPageId().getPageNumber();
            count = 0;
            keyIndex = 0;
            keyValueFreePosition = currentPage.getMaxContentSize();
            freeSpaceInPage = keyValueFreePosition -
                (getHeaderSize(pageNum) + INT_SERIALIZED_SIZE);
        } else {
            break;  // All values written
        }
    } while (true);
}
```

**Put Algorithm Complexity**:
1. **Lookup**: O(log n) via binary search (lookupInPage)
2. **Serialization**: O(k) where k = number of values
3. **Page Insertion**: O(1) at end of mutable page, O(n) if middle insertion
4. **Typically**: O(1) for puts (most go to newest page)

**Put Scenarios**:
- **Happy Path**: Values fit in current page → single page write
- **Multi-Value Split**: Too many values → split across multiple pages
- **Page Full**: Current page full → create new page
- **Existing Key**: Multiple values for same key → append to value array

### 6.3 Put vs Transaction Put

**Immediate Put** (no transaction):
```
lock.read() {
    mutable.internalPut(keys, rids)  // Direct page modification
}
```

**Deferred Put** (in transaction):
```
tx.addIndexOperation(this, ADD, keys, rid)
    ↓ (at commit time, in write lock)
lock.write() {
    mutable.internalPut(keys, rids)  // Applied with other deferred ops
}
```

**Advantages of Deferral**:
- Atomicity: All index changes applied together
- Consistency: No partial index state visible
- Better Throughput: Batch operations together
- Rollback Support: Discard on abort

---

## 7. NULL_STRATEGY IMPLEMENTATION

### 7.1 NULL_STRATEGY Enumeration

```java
public enum NULL_STRATEGY {
    ERROR,   // Throw exception if NULL key encountered
    SKIP     // Skip indexing records with NULL keys
}
```

**Definition** (LSMTreeIndexAbstract.java line 63):
```java
public enum NULL_STRATEGY {ERROR, SKIP}
```

### 7.2 Enforcement Points

**1. At Index Definition**:
```java
protected LSMTreeIndexAbstract(..., final NULL_STRATEGY nullStrategy) {
    if (nullStrategy == null)
        throw new IllegalArgumentException("Index null strategy is null");
    this.nullStrategy = nullStrategy;
}
```

**2. During Put Operation**:
```java
protected void checkForNulls(final Object[] keys) {
    if (nullStrategy != NULL_STRATEGY.ERROR)
        return;  // SKIP mode: no error

    if (keys != null)
        for (int i = 0; i < keys.length; ++i)
            if (keys[i] == null)
                throw new IllegalArgumentException(
                    "Indexed key " + mainIndex.getTypeName() +
                    mainIndex.propertyNames + " cannot be NULL (" +
                    Arrays.toString(keys) + ")");
}
```

**3. During Indexing**:
```java
// In LSMTreeIndexMutable.internalPut():
checkForNulls(keys);

final Object[] convertedKeys = convertKeys(keys, binaryKeyTypes);
if (convertedKeys == null && nullStrategy == NULL_STRATEGY.SKIP)
    return;  // Skip the entry
```

**4. During Get (SKIP mode)**:
```java
// In LSMTreeIndexMutable.get():
final Object[] convertedKeys = convertKeys(keys, binaryKeyTypes);
if (convertedKeys == null && nullStrategy == NULL_STRATEGY.SKIP)
    return new TempIndexCursor(Collections.emptyList());
```

**5. During Compacted Get**:
```java
// In LSMTreeIndexCompacted.get():
final Object[] convertedKeys = convertKeys(keys, binaryKeyTypes);
if (convertedKeys == null && nullStrategy == NULL_STRATEGY.SKIP)
    return Collections.emptySet();
```

### 7.3 Key Behavior

**SKIP Strategy** (default):
```
Index Definition: NULL_STRATEGY.SKIP
    ↓
Document with NULL property value:
    → Record NOT added to index
    → No error thrown
    → Index query doesn't return this record

Use Case: Optional fields, allows sparse indexing
```

**ERROR Strategy**:
```
Index Definition: NULL_STRATEGY.ERROR
    ↓
Document with NULL property value:
    → IllegalArgumentException thrown
    → Transaction rolled back
    → Record NOT saved

Use Case: Required fields, enforce NOT NULL constraint
```

### 7.4 Null Handling in Serialization

**Serialized Format** (with NULL markers):
```java
// Each key in composite key serialized with NULL flag
for (int i = 0; i < binaryKeyTypes.length; ++i) {
    final Object value = keys[i];

    if (value == null)
        buffer.putByte((byte) 0);  // NULL marker
    else {
        buffer.putByte((byte) 1);  // NOT NULL marker
        serializer.serializeValue(database, buffer,
            binaryKeyTypes[i], value);
    }
}
```

**Deserialization** (with NULL handling):
```java
for (int keyIndex = 0; keyIndex < binaryKeyTypes.length; ++keyIndex) {
    final boolean notNull =
        version < 1 || buffer.getByte() == 1;
    if (notNull)
        key[keyIndex] = serializer.deserializeValue(database,
            buffer, binaryKeyTypes[keyIndex], null);
    else
        key[keyIndex] = null;  // NULL value
}
```

**Comparison with NULLs**:
```java
// NULL comparison logic
for (int keyIndex = 0; keyIndex < keys.length; ++keyIndex) {
    final Object key = keys[keyIndex];

    final boolean notNull = version < 1 || buffer.getByte() == 1;
    if (!notNull) {
        if (key == null)
            continue;  // Both NULL: equal
        else
            return 1;  // Page NULL > search NOT NULL
    }

    if (key == null)
        return -1;  // Search NULL < page NOT NULL

    // Both not null: normal comparison
    result = comparator.compare(key, keyTypes[keyIndex],
        pageValue, keyTypes[keyIndex]);
}
```

### 7.5 Usage Example

**Define Index with NULL Strategy**:
```java
// ERROR: Require non-null
schema.createIndex("User.email", "UNIQUE", null,
    "NULL_STRATEGY", "ERROR");

// SKIP: Allow nulls (default)
schema.createIndex("User.nickname", null, null,
    "NULL_STRATEGY", "SKIP");
```

**Via IndexBuilder**:
```java
IndexBuilder builder = schema.createIndexBuilder("email")
    .onType(User.class)
    .onProperty("email")
    .unique()
    .nullStrategy(NULL_STRATEGY.ERROR);

Index index = builder.build();
```

---

## ARCHITECTURAL PATTERNS FOR NEW INDEX TYPES

### Key Patterns to Follow

1. **Extend LSMTreeIndexAbstract** (if following LSM-tree approach)
   - Implement `compareKey()` abstract method
   - Override `lookupInPage()` if different logic needed
   - Use transaction integration from base class

2. **Implement IndexInternal Interface**
   - Implement RangeIndex if supporting range queries
   - Provide get(), put(), remove(), iterator() methods
   - Manage INDEX_STATUS lifecycle

3. **Create Inner IndexFactoryHandler**
   - Static inner class implementing IndexFactoryHandler
   - Registered in LocalSchema during startup

4. **Create Inner PaginatedComponentFactoryHandler**
   - One for unique indexes
   - One for non-unique indexes
   - Registered with ComponentFactory

5. **Use Consistent File Extensions**
   - unique_type_suffix format: e.g., "umtidx" (unique mutable), "uctidx" (unique compacted)
   - non-unique: "numtidx", "nuctidx"

6. **Leverage Transaction System**
   - Use RWLockContext for concurrency
   - Defer operations within transactions
   - Merge results with transaction changes

7. **Support NULL_STRATEGY**
   - Check at put() time
   - Skip or error based on strategy
   - Handle NULL keys in serialization

8. **Page-Based Storage**
   - Extend PaginatedComponent
   - Manage page layout and free space
   - Use Binary/TrackableBinary for serialization
   - Track modifications in transaction
