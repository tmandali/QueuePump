using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Threading.Tasks;

namespace QueueProcessor
{
    public class Envelope
    {
        public Uri EndPoint { get; set; }

        public string ReplyTo { get; set; }

        public dynamic Headers { get; set; }


        public void PrepareExportCommand(SqlCommand command)
        {
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.AddWithValue("@To", EndPoint.ToString());

            var headers = (ICollection<KeyValuePair<string, object>>)Headers;
            foreach (var prm in headers)
            {
                command.Parameters.AddWithValue('@' + prm.Key, prm.Value ?? DBNull.Value);
            }
        }

        static T TryGetHeaderValue<T>(Dictionary<string, string> headers, string name, Func<string, T> conversion)
        {
            string text;
            if (!headers.TryGetValue(name, out text))
            {
                return default(T);
            }
            var value = conversion(text);
            return value;
        }


        public static async Task<Envelope> Read(SqlDataReader dataReader)
        {
            var result = await ReadRow(dataReader);
            return result.TryParse();
        }

        Envelope TryParse()
        {
            try
            {
                if (EndPoint.Scheme != "mssql")
                    throw new Exception("Desteklenmeyen endpoint !");
                return this;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        static async Task<Envelope> ReadRow(SqlDataReader dataReader)
        {
            var envelope = new Envelope();
            envelope.Headers = new ExpandoObject();

            var headers = (ICollection<KeyValuePair<string, object>>)envelope.Headers;
            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                string name = dataReader.GetName(i);

                if (name == "EndPoint")
                    envelope.EndPoint = new Uri(await dataReader.GetFieldValueAsync<string>(i).ConfigureAwait(false));
                else if (name == "ReplyTo")
                    envelope.ReplyTo = await dataReader.GetFieldValueAsync<string>(i).ConfigureAwait(false);
                else
                    headers.Add(new KeyValuePair<string, object>(name, await GetNullableAsync<object>(dataReader, i)));
            }

            envelope.Headers = headers;
            return envelope;
        }

        static async Task<T> GetNullableAsync<T>(SqlDataReader dataReader, int index) where T : class
        {
            if (await dataReader.IsDBNullAsync(index).ConfigureAwait(false))
            {
                return default(T);
            }

            return await dataReader.GetFieldValueAsync<T>(index).ConfigureAwait(false);
        }
    }
}
