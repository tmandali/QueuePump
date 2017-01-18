using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace QueueProcessor
{
    class SqlConnectionFactory
    {
        public SqlConnectionFactory(Func<Task<SqlConnection>> factory)
        {
            openNewConnection = factory;
        }

        public async Task<SqlConnection> OpenNewConnection()
        {
            var connection = await openNewConnection().ConfigureAwait(false);

            ValidateConnectionPool(connection.ConnectionString);

            return connection;
        }

        public static SqlConnectionFactory Default(string connectionString)
        {
            return new SqlConnectionFactory(async () =>
            {
                ValidateConnectionPool(connectionString);

                var connection = new SqlConnection(connectionString);
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    connection.Dispose();
                    throw;
                }

                return connection;
            });
        }

        static void ValidateConnectionPool(string connectionString)
        {
            if (hasValidated) return;

            var validationResult = ConnectionPoolValidator.Validate(connectionString);
            if (!validationResult.IsValid)
            {
                Trace.TraceWarning(validationResult.Message);
            }

            hasValidated = true;
        }

        Func<Task<SqlConnection>> openNewConnection;
        static bool hasValidated;

        class ConnectionPoolValidator
        {
            public static ValidationCheckResult Validate(string connectionString)
            {
                var keys = new DbConnectionStringBuilder { ConnectionString = connectionString };
                var parsedConnection = new SqlConnectionStringBuilder(connectionString);

                if (keys.ContainsKey("Pooling") && !parsedConnection.Pooling)
                {
                    return ValidationCheckResult.Valid();
                }

                if (!keys.ContainsKey("Max Pool Size") || !keys.ContainsKey("Min Pool Size"))
                {
                    return ValidationCheckResult.Invalid(ConnectionPoolSizeNotSet);
                }

                return ValidationCheckResult.Valid();
            }

            const string ConnectionPoolSizeNotSet =
                "Minimum and Maximum connection pooling values are not " +
                "configured on the provided connection string.";
        }

        class ValidationCheckResult
        {
            ValidationCheckResult(bool valid, string message)
            {
                IsValid = valid;
                Message = message;
            }

            public static ValidationCheckResult Valid()
            {
                return new ValidationCheckResult(true, null);
            }

            public static ValidationCheckResult Invalid(string message)
            {
                return new ValidationCheckResult(false, message);
            }

            public bool IsValid { get; private set; }
            public string Message { get; private set; }
        }
    }
}
