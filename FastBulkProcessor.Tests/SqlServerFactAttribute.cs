namespace FastBulkProcessor.Tests;

public sealed class SqlServerFactAttribute : FactAttribute
{
    private const string ConnectionStringEnvironmentVariable =
        "FAST_BULK_PROCESSOR_SQL_CONNECTION_STRING";

    public SqlServerFactAttribute()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            ConnectionStringEnvironmentVariable,
            EnvironmentVariableTarget.Process);

        if (string.IsNullOrWhiteSpace(connectionString) && OperatingSystem.IsWindows())
        {
            connectionString = Environment.GetEnvironmentVariable(
                ConnectionStringEnvironmentVariable,
                EnvironmentVariableTarget.User);
        }

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Skip = $"未設定環境變數 {ConnectionStringEnvironmentVariable}。";
        }
    }
}
