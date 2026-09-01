using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace FastBulkProcessor.Tests;

public class AdoStreamingReaderTests
{
    /// <summary>
    /// 驗證 ADO.NET 資料來源尚未全部讀取完成前，呼叫端即可取得完成映射的第一筆資料，
    /// 且取得第一筆時只會開啟一次連線、執行一次命令、讀取並映射一筆資料。
    /// </summary>
    [Fact]
    public async Task ReadAsync_取得第一筆時_只讀取並映射一筆()
    {
        var dataReader = new TrackingDbDataReader(totalCount: 500_000);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var mapCount = 0;
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            row =>
            {
                mapCount++;
                return row.GetInt32(0);
            });

        await using var enumerator = sut.ReadAsync().GetAsyncEnumerator();

        var hasItem = await enumerator.MoveNextAsync();

        Assert.True(hasItem);
        Assert.Equal(1, enumerator.Current);
        Assert.Equal(1, connection.OpenCount);
        Assert.Equal(1, command.ExecuteReaderCount);
        Assert.Equal(1, dataReader.ReadCount);
        Assert.Equal(1, mapCount);
    }

    /// <summary>
    /// 驗證持續列舉 ADO.NET 串流時，所有資料都會依來源順序逐筆讀取並完成映射。
    /// </summary>
    [Fact]
    public async Task ReadAsync_持續列舉時_依序讀取並映射所有資料()
    {
        var dataReader = new TrackingDbDataReader(totalCount: 3);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var mapCount = 0;
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            row =>
            {
                mapCount++;
                return row.GetInt32(0) * 10;
            });
        var results = new List<int>();

        await foreach (var item in sut.ReadAsync())
        {
            results.Add(item);
        }

        Assert.Equal([10, 20, 30], results);
        Assert.Equal(1, connection.OpenCount);
        Assert.Equal(1, command.ExecuteReaderCount);
        Assert.Equal(4, dataReader.ReadCount);
        Assert.Equal(3, mapCount);
    }

    /// <summary>
    /// 驗證 ADO.NET 資料來源沒有任何資料時，回傳空串流且不會執行資料映射。
    /// </summary>
    [Fact]
    public async Task ReadAsync_來源沒有資料時_回傳空串流且不執行映射()
    {
        var dataReader = new TrackingDbDataReader(totalCount: 0);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var mapCount = 0;
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            row =>
            {
                mapCount++;
                return row.GetInt32(0);
            });
        var results = new List<int>();

        await foreach (var item in sut.ReadAsync())
        {
            results.Add(item);
        }

        Assert.Empty(results);
        Assert.Equal(1, connection.OpenCount);
        Assert.Equal(1, command.ExecuteReaderCount);
        Assert.Equal(1, dataReader.ReadCount);
        Assert.Equal(0, mapCount);
    }

    /// <summary>
    /// 驗證 ADO.NET 串流正常讀取完成後，資料讀取器、命令與連線都會被正確釋放。
    /// </summary>
    [Fact]
    public async Task ReadAsync_正常讀取完成時_釋放所有資料庫資源()
    {
        var dataReader = new TrackingDbDataReader(totalCount: 1);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            row => row.GetInt32(0));

        await foreach (var _ in sut.ReadAsync())
        {
        }

        Assert.True(dataReader.IsDisposed);
        Assert.True(command.IsDisposed);
        Assert.True(connection.IsDisposed);
    }

    /// <summary>
    /// 驗證呼叫端取得第一筆資料後提前停止列舉時，資料讀取器、命令與連線仍會被正確釋放。
    /// </summary>
    [Fact]
    public async Task ReadAsync_取得第一筆後停止列舉時_釋放所有資料庫資源()
    {
        var dataReader = new TrackingDbDataReader(totalCount: 500_000);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            row => row.GetInt32(0));

        await foreach (var _ in sut.ReadAsync())
        {
            break;
        }

        Assert.Equal(1, dataReader.ReadCount);
        Assert.True(dataReader.IsDisposed);
        Assert.True(command.IsDisposed);
        Assert.True(connection.IsDisposed);
    }

    /// <summary>
    /// 驗證串流讀取過程被取消時，停止讀取並拋出取消例外，且所有資料庫資源都會被正確釋放。
    /// </summary>
    [Fact]
    public async Task ReadAsync_讀取過程被取消時_停止讀取並釋放所有資料庫資源()
    {
        var dataReader = new TrackingDbDataReader(totalCount: 500_000);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var mapCount = 0;
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            row =>
            {
                mapCount++;
                return row.GetInt32(0);
            });
        using var cancellationTokenSource = new CancellationTokenSource();

        await using (var enumerator = sut
                         .ReadAsync(cancellationTokenSource.Token)
                         .GetAsyncEnumerator())
        {
            Assert.True(await enumerator.MoveNextAsync());

            cancellationTokenSource.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(
                async () => await enumerator.MoveNextAsync().AsTask());
        }

        Assert.Equal(1, dataReader.ReadCount);
        Assert.Equal(1, mapCount);
        Assert.True(dataReader.IsDisposed);
        Assert.True(command.IsDisposed);
        Assert.True(connection.IsDisposed);
    }

    /// <summary>
    /// 驗證資料映射發生例外時，原始例外會向外傳遞，且所有資料庫資源都會被正確釋放。
    /// </summary>
    [Fact]
    public async Task ReadAsync_資料映射發生例外時_傳遞原始例外並釋放所有資料庫資源()
    {
        var dataReader = new TrackingDbDataReader(totalCount: 3);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var expectedException = new InvalidOperationException("Mapping failed.");
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            _ => throw expectedException);

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync())
                {
                }
            });

        Assert.Same(expectedException, actualException);
        Assert.Equal(1, dataReader.ReadCount);
        Assert.True(dataReader.IsDisposed);
        Assert.True(command.IsDisposed);
        Assert.True(connection.IsDisposed);
    }

    /// <summary>
    /// 驗證資料庫讀取發生例外時，原始例外會向外傳遞、不會執行映射，
    /// 且所有資料庫資源都會被正確釋放。
    /// </summary>
    [Fact]
    public async Task ReadAsync_資料庫讀取發生例外時_傳遞原始例外並釋放所有資料庫資源()
    {
        var expectedException = new InvalidOperationException("Database read failed.");
        var dataReader = new TrackingDbDataReader(
            totalCount: 3,
            readException: expectedException);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var mapCount = 0;
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            row =>
            {
                mapCount++;
                return row.GetInt32(0);
            });

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync())
                {
                }
            });

        Assert.Same(expectedException, actualException);
        Assert.Equal(0, mapCount);
        Assert.True(dataReader.IsDisposed);
        Assert.True(command.IsDisposed);
        Assert.True(connection.IsDisposed);
    }

    /// <summary>
    /// 驗證只建立 ADO.NET 資料串流及列舉器、尚未要求第一筆資料時，不會建立連線或存取資料庫。
    /// </summary>
    [Fact]
    public async Task ReadAsync_尚未要求第一筆資料時_不建立連線且不存取資料庫()
    {
        var dataReader = new TrackingDbDataReader(totalCount: 3);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(command);
        var connectionFactoryCount = 0;
        var commandFactoryCount = 0;
        var mapCount = 0;
        var sut = new AdoStreamingReader<int>(
            () =>
            {
                connectionFactoryCount++;
                return connection;
            },
            _ =>
            {
                commandFactoryCount++;
                return command;
            },
            row =>
            {
                mapCount++;
                return row.GetInt32(0);
            });

        var stream = sut.ReadAsync();
        await using var enumerator = stream.GetAsyncEnumerator();

        Assert.Equal(0, connectionFactoryCount);
        Assert.Equal(0, connection.OpenCount);
        Assert.Equal(0, commandFactoryCount);
        Assert.Equal(0, command.ExecuteReaderCount);
        Assert.Equal(0, dataReader.ReadCount);
        Assert.Equal(0, mapCount);
    }

    /// <summary>
    /// 驗證查詢命令執行發生例外時，原始例外會向外傳遞、不會讀取或映射資料，
    /// 且已建立的命令與連線都會被正確釋放。
    /// </summary>
    [Fact]
    public async Task ReadAsync_查詢命令執行發生例外時_傳遞原始例外並釋放已建立資源()
    {
        var expectedException = new InvalidOperationException("Command execution failed.");
        var dataReader = new TrackingDbDataReader(totalCount: 3);
        var command = new TrackingDbCommand(
            dataReader,
            executeReaderException: expectedException);
        var connection = new TrackingDbConnection(command);
        var mapCount = 0;
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ => command,
            row =>
            {
                mapCount++;
                return row.GetInt32(0);
            });

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync())
                {
                }
            });

        Assert.Same(expectedException, actualException);
        Assert.Equal(1, connection.OpenCount);
        Assert.Equal(1, command.ExecuteReaderCount);
        Assert.Equal(0, dataReader.ReadCount);
        Assert.Equal(0, mapCount);
        Assert.True(command.IsDisposed);
        Assert.True(connection.IsDisposed);
    }

    /// <summary>
    /// 驗證資料庫連線開啟發生例外時，原始例外會向外傳遞、不會建立命令或讀取資料，
    /// 且已由工廠建立的連線仍會被正確釋放。
    /// </summary>
    [Fact]
    public async Task ReadAsync_資料庫連線開啟發生例外時_傳遞原始例外並釋放連線()
    {
        var expectedException = new InvalidOperationException("Connection open failed.");
        var dataReader = new TrackingDbDataReader(totalCount: 3);
        var command = new TrackingDbCommand(dataReader);
        var connection = new TrackingDbConnection(
            command,
            openException: expectedException);
        var commandFactoryCount = 0;
        var mapCount = 0;
        var sut = new AdoStreamingReader<int>(
            () => connection,
            _ =>
            {
                commandFactoryCount++;
                return command;
            },
            row =>
            {
                mapCount++;
                return row.GetInt32(0);
            });

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            async () =>
            {
                await foreach (var _ in sut.ReadAsync())
                {
                }
            });

        Assert.Same(expectedException, actualException);
        Assert.Equal(1, connection.OpenCount);
        Assert.Equal(0, commandFactoryCount);
        Assert.Equal(0, command.ExecuteReaderCount);
        Assert.Equal(0, dataReader.ReadCount);
        Assert.Equal(0, mapCount);
        Assert.True(connection.IsDisposed);
    }

    private sealed class TrackingDbConnection(
        TrackingDbCommand command,
        Exception? openException = null) : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;

        public int OpenCount { get; private set; }
        public bool IsDisposed { get; private set; }
        [AllowNull]
        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => "TestDatabase";
        public override string DataSource => "TestDataSource";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) =>
            throw new NotSupportedException();

        public override void Close() => _state = ConnectionState.Closed;

        public override void Open()
        {
            OpenCount++;

            if (openException is not null)
            {
                throw openException;
            }

            _state = ConnectionState.Open;
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Open();
            return Task.CompletedTask;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
            throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => command;

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                IsDisposed = true;
            }

            base.Dispose(disposing);
        }
    }

    private sealed class TrackingDbCommand(
        TrackingDbDataReader dataReader,
        Exception? executeReaderException = null) : DbCommand
    {
        public int ExecuteReaderCount { get; private set; }
        public bool IsDisposed { get; private set; }
        [AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection =>
            throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }

        public override void Cancel() => throw new NotSupportedException();
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() => throw new NotSupportedException();
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            ExecuteReaderCount++;
            return dataReader;
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ExecuteReaderCount++;

            if (executeReaderException is not null)
            {
                return Task.FromException<DbDataReader>(executeReaderException);
            }

            return Task.FromResult<DbDataReader>(dataReader);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                IsDisposed = true;
            }

            base.Dispose(disposing);
        }
    }

    private sealed class TrackingDbDataReader(
        int totalCount,
        Exception? readException = null) : DbDataReader
    {
        private int _currentValue;

        public int ReadCount { get; private set; }
        public bool IsDisposed { get; private set; }
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));
        public override int Depth => 0;
        public override int FieldCount => 1;
        public override bool HasRows => totalCount > 0;
        public override bool IsClosed => IsDisposed;
        public override int RecordsAffected => -1;

        public override bool Read()
        {
            ReadCount++;
            _currentValue++;
            return _currentValue <= totalCount;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (readException is not null)
            {
                return Task.FromException<bool>(readException);
            }

            return Task.FromResult(Read());
        }

        public override int GetInt32(int ordinal) =>
            ordinal == 0 ? _currentValue : throw new IndexOutOfRangeException();

        public override string GetName(int ordinal) =>
            ordinal == 0 ? "Value" : throw new IndexOutOfRangeException();

        public override int GetOrdinal(string name) =>
            name == "Value" ? 0 : throw new IndexOutOfRangeException();

        public override object GetValue(int ordinal) => GetInt32(ordinal);

        public override int GetValues(object[] values)
        {
            if (values.Length == 0)
            {
                return 0;
            }

            values[0] = _currentValue;
            return 1;
        }

        public override bool IsDBNull(int ordinal) => false;
        public override bool NextResult() => false;
        public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => typeof(int).Name;
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override Type GetFieldType(int ordinal) => typeof(int);
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override long GetInt64(int ordinal) => throw new NotSupportedException();
        public override string GetString(int ordinal) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                IsDisposed = true;
            }

            base.Dispose(disposing);
        }
    }
}
