import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import {
    createDatabase,
    cleanupDatabase,
    uniqueDbName,
    command,
    query
} from './helpers.js';

describe('ULTRA MINIMAL ArcadeDB Bug - Plain SQL Only', () => {
    const dbName = uniqueDbName('ultra_minimal');

    beforeAll(async () => {
        await cleanupDatabase(dbName);
        await createDatabase(dbName);
    });

    afterAll(async () => {
        await cleanupDatabase(dbName);
    });

    test('absolute minimal reproduction', async () => {
        console.log('\n=== SETUP ===');

        // 1. Create two types with LINK and COMPOSITE INDEX like HostCard
        await command(dbName, `CREATE DOCUMENT TYPE Parent`);
        await command(dbName, `CREATE DOCUMENT TYPE Child`);
        await command(dbName, `CREATE PROPERTY Child.uid STRING`);
        await command(dbName, `CREATE PROPERTY Child.status STRING (default 'synced')`);
        await command(dbName, `CREATE PROPERTY Child.version INTEGER (default 1)`);
        await command(dbName, `CREATE PROPERTY Child.parent LINK Parent`);

        // THE KEY: Composite index on (status, version) like HostCard has on (syncStatus, baseVersion)
        // await command(dbName, `CREATE INDEX ON Child (status, version) NOTUNIQUE`);
        await command(dbName, `CREATE INDEX ON Child (status) NOTUNIQUE`);

        // 2. Create parent
        const p = await command(dbName, `INSERT INTO Parent SET name = 'p1' RETURN @this`);
        const pRid = p.result[0]['@rid'];

        // 3. Insert 3 children WITHOUT explicit status (use default)
        await command(dbName, `INSERT INTO Child SET uid = 'c1', parent = ${pRid}`);
        await command(dbName, `INSERT INTO Child SET uid = 'c2', parent = ${pRid}`);
        await command(dbName, `INSERT INTO Child SET uid = 'c3', parent = ${pRid}`);

        console.log('Created 3 children with default status=synced');

        // 4. Mark c1 and c2 as pending
        await command(dbName, `UPDATE Child SET status = 'pending' WHERE uid = 'c1'`);
        await command(dbName, `UPDATE Child SET status = 'pending' WHERE uid = 'c2'`);

        console.log('\n=== BEFORE BUG ===');

        // 5. Verify WHERE works - should find 2 pending
        const before = await query(dbName, `SELECT uid, status FROM Child WHERE status = 'pending'`);
        console.log('Pending (WHERE):', before.result.length, '→', before.result.map(r => r.uid));
        expect(before.result.length).toBe(2);

        console.log('\n=== TRIGGER BUG ===');

        // 6. Update c1 with parameterized multi-field UPDATE (including version field in composite index)
        await command(dbName, `UPDATE Child SET version = :version, status = :status WHERE uid = :uid`, {
            uid: 'c1',
            version: 2,
            status: 'synced'
        });

        console.log('Updated c1 to synced (multi-field parameterized UPDATE)');

        console.log('\n=== AFTER BUG ===');

        // 7. Check without WHERE - should show c2 pending, c1+c3 synced
        const all = await query(dbName, `SELECT uid, status FROM Child ORDER BY uid`);
        console.log('All (no WHERE):', all.result);

        // 8. BUG: WHERE status='pending' should find c2 but finds 0
        const pending = await query(dbName, `SELECT uid, status FROM Child WHERE status = 'pending'`);
        console.log('Pending (WHERE):', pending.result.length, '→', pending.result.map(r => r.uid));

        // 9. BUG: WHERE status='synced' should find c1+c3 but finds only c1
        const synced = await query(dbName, `SELECT uid, status FROM Child WHERE status = 'synced'`);
        console.log('Synced (WHERE):', synced.result.length, '→', synced.result.map(r => r.uid));

        console.log('\n=== RESULT ===');
        console.log(`Pending: expected 1 (c2), got ${pending.result.length}`);
        console.log(`Synced: expected 2 (c1,c3), got ${synced.result.length}`);

        expect(pending.result.length).toBe(1);
        expect(synced.result.length).toBe(2);
    });
});
