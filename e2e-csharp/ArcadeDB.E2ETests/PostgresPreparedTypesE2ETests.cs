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

using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace ArcadeDB.E2ETests;

/// <summary>
/// Issue #11838: over the Postgres wire protocol a prepared statement described the columns of schemaless properties as
/// varchar (text[] for a list), so a client reading through a server-prepared statement got true as "t", 42 as "42", 2.5
/// as "2.5" and [1, 2, 3] as {"1", "2", "3"}, where the same statement unprepared returned typed values. A uuid parameter
/// (OID 2950) could not be bound at all.
/// <para>
/// Npgsql prepares on the server only when asked: explicitly through <c>Prepare()</c>, or by auto-preparation once a
/// statement has been used <c>Auto Prepare Min Usages</c> times (5 by default: the 5th execution is the first prepared one). Either way it
/// sends Parse + Describe('S') once and then Bind + Execute, decoding every later execution's rows with the statement's
/// description.
/// </para>
/// </summary>
[Collection("ArcadeDB")]
public class PostgresPreparedTypesE2ETests
{
    private const string Type = "T11838";
    private const string Query = "SELECT v FROM " + Type + " WHERE name = $1";
    private const int Rounds = 8; // past every driver's prepare threshold (Npgsql and pgjdbc 5th, psycopg 6th)
    private static readonly Guid Id = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
    private static readonly string[] Names = ["bool", "int", "double", "list"];

    private static readonly SemaphoreSlim SeedLock = new(1, 1);
    private static bool _seeded;

    private readonly ArcadeDbFixture _fixture;

    public PostgresPreparedTypesE2ETests(ArcadeDbFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task SeedAsync()
    {
        await SeedLock.WaitAsync();
        try
        {
            if (_seeded)
                return;
            await _fixture.HttpCommandAsync($"{{\"language\":\"sql\",\"command\":\"CREATE DOCUMENT TYPE {Type}\"}}");
            // no property is declared: every column read below is schemaless
            await _fixture.HttpCommandAsync(
                $"{{\"language\":\"sql\",\"command\":\"INSERT INTO {Type} CONTENT :r\",\"params\":{{\"r\":[" +
                "{\"name\":\"bool\",\"v\":true}," +
                "{\"name\":\"int\",\"v\":42}," +
                "{\"name\":\"double\",\"v\":2.5}," +
                "{\"name\":\"list\",\"v\":[1,2,3]}," +
                $"{{\"name\":\"uuid\",\"u\":\"{Id}\"}}]}}}}");
            _seeded = true;
        }
        finally
        {
            SeedLock.Release();
        }
    }

    private static void AssertNative(string name, int execution, NpgsqlDataReader reader)
    {
        var value = reader.GetValue(0);
        var where = $"{name} execution {execution}: column {reader.GetDataTypeName(0)}, value {value} ({value.GetType().Name})";
        switch (name)
        {
            case "bool":
                Assert.True(value is bool b && b, where);
                break;
            case "int":
                Assert.True(value is int i && i == 42, where);
                break;
            case "double":
                Assert.True(value is double d && d == 2.5, where);
                break;
            case "list":
                Assert.True(
                    (value is int[] ints && ints.SequenceEqual([1, 2, 3])) || (value is long[] longs && longs.SequenceEqual([1L, 2L, 3L])),
                    where);
                break;
        }
    }

    private static async Task ReadAndAssertAsync(NpgsqlCommand select, string name, int execution)
    {
        select.Parameters[0].Value = name;
        await using var reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), $"{name} execution {execution}: no row");
        AssertNative(name, execution, reader);
        Assert.False(await reader.ReadAsync(), $"{name} execution {execution}: more than one row");
    }

    [Fact]
    public async Task ExplicitlyPreparedStatementKeepsNativeTypes()
    {
        await SeedAsync();
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();
        await using var select = new NpgsqlCommand(Query, conn);
        select.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
        await select.PrepareAsync();
        Assert.True(select.IsPrepared);

        // one server-prepared statement serving rows of a different type on each execution
        for (var execution = 1; execution <= Rounds; execution++)
            foreach (var name in Names)
                await ReadAndAssertAsync(select, name, execution);
    }

    [Fact]
    public async Task AutoPreparedStatementKeepsNativeTypesPastTheThreshold()
    {
        await SeedAsync();
        var builder = new NpgsqlConnectionStringBuilder(_fixture.ConnectionString) { MaxAutoPrepare = 10, AutoPrepareMinUsages = 5 };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        await using var conn = await dataSource.OpenConnectionAsync();

        foreach (var name in Names)
        {
            await using var select = new NpgsqlCommand(Query, conn);
            select.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            // executions 1-4 unprepared, 5-8 auto-prepared on the server
            for (var execution = 1; execution <= Rounds; execution++)
                await ReadAndAssertAsync(select, name, execution);
        }
    }

    [Fact]
    public async Task UuidParameterBinds()
    {
        await SeedAsync();
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();

        // Npgsql declares a Guid as uuid (OID 2950) and sends its 16 bytes in binary, unprepared and prepared
        foreach (var prepare in new[] { false, true })
        {
            await using var select = new NpgsqlCommand($"SELECT name, u FROM {Type} WHERE u = $1", conn);
            select.Parameters.Add(new NpgsqlParameter { Value = Id });
            if (prepare)
                await select.PrepareAsync();
            for (var execution = 1; execution <= Rounds; execution++)
            {
                await using var reader = await select.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync(), $"prepared={prepare} execution {execution}: no row");
                Assert.Equal("uuid", reader.GetString(0));
                Assert.Equal(Id.ToString(), reader.GetString(1));
            }
        }
    }

    [Fact]
    public async Task UuidParameterRoundTripsThroughAnInsert()
    {
        await SeedAsync();
        var written = Guid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();

        await using (var insert = new NpgsqlCommand($"INSERT INTO {Type} SET name = 'uuid-written', u = $1", conn))
        {
            insert.Parameters.Add(new NpgsqlParameter { Value = written });
            await using var reader = await insert.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
            }
        }

        await using var select = new NpgsqlCommand($"SELECT u FROM {Type} WHERE name = 'uuid-written' AND u = $1", conn);
        select.Parameters.Add(new NpgsqlParameter { Value = written });
        await using (var reader = await select.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            Assert.Equal(written.ToString(), reader.GetString(0));
            Assert.Equal(written, Guid.Parse(reader.GetString(0)));
        }
    }
}
