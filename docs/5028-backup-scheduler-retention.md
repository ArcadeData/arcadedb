# Issue #5028 - Backup scheduler & retention fixes

## Symptom
Four defects in the `server` backup subsystem:
1. Cron parser ANDs day-of-month / day-of-week when both restricted (standard cron ORs them) -> backups run far less often than configured.
2. No field range validation: out-of-range fields (hour 24, dow 7, dom 32...) parse but never match, and `getNextExecutionTime` throws `IllegalStateException` inside the reschedule chain -> all future backups silently stop. Increment `x/0` loops forever, hanging the scheduler thread at config load.
3. Cancel vs cron-reschedule race: an in-flight `BackupTask` can resurrect a cancelled schedule, leaving duplicate schedules (double backups). Shutdown can throw `RejectedExecutionException` in the worker.
4. Tiered retention keeps the OLDEST file per bucket and runs right after each backup, so the just-completed newest backup is deleted seconds after creation.
   - Minor: `BackupTask.performBackup` checks `database.isTransactionActive()` on the backup thread's own thread-local tx, which is never active -> dead code / false confidence.

## Root cause
- `CronScheduleParser.getNextExecutionTime` uses AND for DOM/DOW and never tracks whether a field is restricted.
- `parseField`/`parseValue` never bounds-check parsed values or reject non-positive increments.
- `BackupScheduler.scheduleNextCronExecution` / `cancelBackup` have no generation token; reschedule put can win the race after a cancel. Delay computation and `executor.schedule` are unguarded.
- `BackupRetentionManager.applyRetention` never unconditionally preserves the most recent file before tier selection.

## Fix
- CronScheduleParser: track `dayOfMonthRestricted` / `dayOfWeekRestricted`; OR them when both restricted, else AND (wildcards match everything). Validate every value is within `[min,max]`; normalize day-of-week `7` -> Sunday(0); require increment > 0; validate range order and increment token shape.
- BackupScheduler: per-database generation token guards reschedule/cancel under a single lock; delay computation and `executor.schedule` wrapped in try/catch (impossible schedule stops only that DB with a clear SEVERE log; RejectedExecutionException on shutdown swallowed).
- BackupRetentionManager: always add the most recent backup to the keep set before deletion.
- BackupTask: remove the dead active-transaction check.

## Tests
- `CronScheduleParserSemanticsTest` - OR semantics, range rejection, `x/0` rejection, dow 7 = Sunday, known-good next-fire table.
- `BackupRetentionNewestBackupTest` - newest backup always survives tiered retention.
- `BackupSchedulerValidationTest` - invalid cron config fails fast (no schedule, no throw); cancel+reschedule leaves exactly one schedule.

## Impact
Scoped to `server` backup package. No API changes. Existing tests unchanged.
