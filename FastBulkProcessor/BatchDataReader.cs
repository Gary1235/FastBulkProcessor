using System.Collections;
using System.Data.Common;

namespace FastBulkProcessor;

/// <summary>
/// 將單一目標資料批次包裝成 ADO.NET 資料讀取器，供 Bulk Copy 依欄位 mapping 逐列取值。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
internal sealed class BatchDataReader<T> : DbDataReader
{
    private readonly IReadOnlyList<T> _batch;
    private readonly IReadOnlyList<BulkColumn<T>> _columns;
    private int _rowIndex = -1;
    private bool _isClosed;

    /// <summary>
    /// 建立指定批次及欄位 mapping 的資料讀取器。
    /// </summary>
    /// <param name="batch">要逐列讀取的單一目標資料批次。</param>
    /// <param name="columns">決定欄位順序、名稱、型別及取值方式的 mapping。</param>
    public BatchDataReader(
        IReadOnlyList<T> batch,
        IReadOnlyList<BulkColumn<T>> columns)
    {
        _batch = batch;
        _columns = columns;
    }

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override int Depth => 0;

    public override int FieldCount => _columns.Count;

    public override bool HasRows => _batch.Count > 0;

    public override bool IsClosed => _isClosed;

    public override int RecordsAffected => -1;

    public override bool Read()
    {
        var nextRowIndex = _rowIndex + 1;

        if (nextRowIndex >= _batch.Count)
        {
            _rowIndex = _batch.Count;
            return false;
        }

        _rowIndex = nextRowIndex;
        return true;
    }

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Read());
    }

    public override string GetName(int ordinal) => GetColumn(ordinal).DestinationName;

    public override int GetOrdinal(string name)
    {
        for (var ordinal = 0; ordinal < _columns.Count; ordinal++)
        {
            if (string.Equals(
                    _columns[ordinal].DestinationName,
                    name,
                    StringComparison.Ordinal))
            {
                return ordinal;
            }
        }

        throw new IndexOutOfRangeException($"找不到名稱為 '{name}' 的欄位。");
    }

    public override Type GetFieldType(int ordinal) => GetColumn(ordinal).DataType;

    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    public override object GetValue(int ordinal)
    {
        EnsureCurrentRow();
        return GetColumn(ordinal).GetValue(_batch[_rowIndex]) ?? DBNull.Value;
    }

    public override int GetValues(object[] values)
    {
        var valueCount = Math.Min(values.Length, FieldCount);

        for (var ordinal = 0; ordinal < valueCount; ordinal++)
        {
            values[ordinal] = GetValue(ordinal);
        }

        return valueCount;
    }

    public override bool IsDBNull(int ordinal) => GetValue(ordinal) is DBNull;

    public override bool NextResult() => false;

    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);

    public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);

    public override long GetBytes(
        int ordinal,
        long dataOffset,
        byte[]? buffer,
        int bufferOffset,
        int length)
    {
        var value = (byte[])GetValue(ordinal);

        if (buffer is null)
        {
            return value.LongLength;
        }

        if (dataOffset >= value.LongLength)
        {
            return 0;
        }

        var copyLength = Math.Min(length, value.Length - checked((int)dataOffset));
        Array.Copy(value, dataOffset, buffer, bufferOffset, copyLength);
        return copyLength;
    }

    public override char GetChar(int ordinal) => (char)GetValue(ordinal);

    public override long GetChars(
        int ordinal,
        long dataOffset,
        char[]? buffer,
        int bufferOffset,
        int length)
    {
        var value = (string)GetValue(ordinal);

        if (buffer is null)
        {
            return value.Length;
        }

        if (dataOffset >= value.Length)
        {
            return 0;
        }

        var copyLength = Math.Min(length, value.Length - checked((int)dataOffset));
        value.CopyTo(checked((int)dataOffset), buffer, bufferOffset, copyLength);
        return copyLength;
    }

    public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);

    public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);

    public override double GetDouble(int ordinal) => (double)GetValue(ordinal);

    public override float GetFloat(int ordinal) => (float)GetValue(ordinal);

    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

    public override short GetInt16(int ordinal) => (short)GetValue(ordinal);

    public override int GetInt32(int ordinal) => (int)GetValue(ordinal);

    public override long GetInt64(int ordinal) => (long)GetValue(ordinal);

    public override string GetString(int ordinal) => (string)GetValue(ordinal);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _isClosed = true;
        }

        base.Dispose(disposing);
    }

    private BulkColumn<T> GetColumn(int ordinal)
    {
        if ((uint)ordinal >= (uint)_columns.Count)
        {
            throw new IndexOutOfRangeException();
        }

        return _columns[ordinal];
    }

    private void EnsureCurrentRow()
    {
        if (_rowIndex < 0 || _rowIndex >= _batch.Count)
        {
            throw new InvalidOperationException("資料讀取器目前沒有可供取值的資料列。");
        }
    }
}
