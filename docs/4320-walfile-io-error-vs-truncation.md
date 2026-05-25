# Fix #4320: WALFile.getTransaction confuses I/O error with truncation

## Issue

`WALFile.getTransaction()` had two bugs:

1. **Short read not detected**: `channel.read(buffer, pos)` return value was never checked.
   A short or zero-byte read left the delta `ByteBuffer` only partially populated, resulting
   in garbage page content silently applied to disk during recovery.

2. **I/O error swallowed**: `catch (Exception e) { return null; }` returned the same `null`
   sentinel used for "end of WAL" or "truncated WAL". Any `IOException` during replay was
   indistinguishable from a clean EOF, causing recovery to stop mid-replay and silently drop
   every committed transaction after the failing read.

## Root Cause

`engine/src/main/java/com/arcadedb/engine/WALFile.java`, lines 203-221 (pre-fix):

```java
final ByteBuffer buffer = ByteBuffer.allocate(deltaSize);
tx.pages[i].currentContent = new Binary(buffer);
channel.read(buffer, pos);   // return value never checked
pos += deltaSize;
...
} catch (final Exception e) {
  return null;               // same value as "truncated WAL"
}
```

## Fix

Two targeted changes in `getTransaction()`:

1. **Loop the delta read** until the buffer is full, treating EOF (`n == -1`) as truncation:
   ```java
   long readPos = pos;
   while (buffer.hasRemaining()) {
     final int n = channel.read(buffer, readPos);
     if (n == -1)
       return null; // truncated WAL: EOF before delta is complete
     readPos += n;
   }
   ```

2. **Split the catch block** so `IOException` re-throws as `WALException` rather than
   returning `null`:
   ```java
   } catch (final IOException e) {
     throw new WALException("Error reading WAL file " + filePath, e);
   } catch (final Exception e) {
     return null;
   }
   ```

## Files Changed

- `engine/src/main/java/com/arcadedb/engine/WALFile.java` - the fix
- `engine/src/test/java/com/arcadedb/engine/WALFileGetTransactionTest.java` - regression tests

## Tests

`WALFileGetTransactionTest` covers three scenarios:

| Test | Expected |
|---|---|
| `validTransactionIsReadCorrectly` | All fields read back correctly |
| `truncatedWalReturnsNull` | Returns `null` at every truncation point |
| `ioErrorOnChannelThrowsWALException` | Throws `WALException`, not `null` |

All 3 passed after the fix. Pre-fix: `ioErrorOnChannelThrowsWALException` failed.

## Impact

- Recovery now throws `WALException` on I/O errors instead of silently stopping.
- Garbage page content from short reads is no longer possible.
- Clean EOF and truncated WAL still return `null` as before (no behaviour change for the
  normal path).
