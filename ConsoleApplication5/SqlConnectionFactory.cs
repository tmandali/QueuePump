using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
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

            //var validationResult = ConnectionPoolValidator.Validate(connectionString);
            //if (!validationResult.IsValid)
            //{
            //    Logger.Warn(validationResult.Message);
            //}

            hasValidated = true;
        }

        Func<Task<SqlConnection>> openNewConnection;
        static bool hasValidated;

    }
}
