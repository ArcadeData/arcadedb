// Test helpers for ArcadeDB
const BASE_URL = 'http://localhost:2480';
const AUTH = 'Basic ' + btoa('root:playwithdata');

export async function serverCommand(command) {
    const res = await fetch(`${BASE_URL}/api/v1/server`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': AUTH
        },
        body: JSON.stringify({ command })
    });
    return res.json();
}

export async function createDatabase(name) {
    return serverCommand(`create database ${name}`);
}

export async function dropDatabase(name) {
    return serverCommand(`drop database ${name}`);
}

export async function databaseExists(name) {
    const res = await fetch(`${BASE_URL}/api/v1/exists/${name}`, {
        headers: { 'Authorization': AUTH }
    });
    const data = await res.json();
    return data.result;
}

export async function query(database, sql, params = null) {
    const body = {
        language: 'sql',
        command: sql
    };
    if (params) {
        body.params = params;
    }

    const res = await fetch(`${BASE_URL}/api/v1/query/${database}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': AUTH
        },
        body: JSON.stringify(body)
    });
    return res.json();
}

export async function command(database, sql, params = null) {
    const body = {
        language: 'sql',
        command: sql
    };
    if (params) {
        body.params = params;
    }

    const res = await fetch(`${BASE_URL}/api/v1/command/${database}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': AUTH
        },
        body: JSON.stringify(body)
    });
    return res.json();
}

export async function beginTransaction(database) {
    const res = await fetch(`${BASE_URL}/api/v1/begin/${database}`, {
        method: 'POST',
        headers: { 'Authorization': AUTH }
    });
    const sessionId = res.headers.get('arcadedb-session-id');
    return sessionId;
}

export async function commitTransaction(database, sessionId) {
    const res = await fetch(`${BASE_URL}/api/v1/commit/${database}`, {
        method: 'POST',
        headers: {
            'Authorization': AUTH,
            'arcadedb-session-id': sessionId
        }
    });
    return res.ok;
}

export async function rollbackTransaction(database, sessionId) {
    const res = await fetch(`${BASE_URL}/api/v1/rollback/${database}`, {
        method: 'POST',
        headers: {
            'Authorization': AUTH,
            'arcadedb-session-id': sessionId
        }
    });
    return res.ok;
}

export async function commandWithSession(database, sessionId, sql, params = null) {
    const body = {
        language: 'sql',
        command: sql
    };
    if (params) {
        body.params = params;
    }

    const res = await fetch(`${BASE_URL}/api/v1/command/${database}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': AUTH,
            'arcadedb-session-id': sessionId
        },
        body: JSON.stringify(body)
    });
    return res.json();
}

export async function queryWithSession(database, sessionId, sql, params = null) {
    const body = {
        language: 'sql',
        command: sql
    };
    if (params) {
        body.params = params;
    }

    const res = await fetch(`${BASE_URL}/api/v1/query/${database}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': AUTH,
            'arcadedb-session-id': sessionId
        },
        body: JSON.stringify(body)
    });
    return res.json();
}

// Generate unique database name for test isolation
export function uniqueDbName(prefix) {
    return `${prefix}_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
}

// Cleanup helper - ensures database is dropped even if it doesn't exist
export async function cleanupDatabase(name) {
    try {
        const exists = await databaseExists(name);
        if (exists) {
            await dropDatabase(name);
        }
    } catch (e) {
        // Ignore errors during cleanup
    }
}

export function logMessage(...args) {
    const adbLog = process.env.ADB_LOG;
    if (adbLog === 'true' || adbLog === '1') {
        console.log(...args);
    }
}