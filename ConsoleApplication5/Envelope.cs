using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Threading.Tasks;
using System.Linq;
using System.Text;
using System.Security.Cryptography;

namespace QueueProcessor
{
    public class Envelope
    {
        public Guid MessageId { get; set; }

        public Uri EndPoint { get; set; }

        public string ReplyTo { get; set; }

        public string Error { get; set; }

        public DateTime DeliveryDate { get; set; }

        public long RowVersion { get; set; }

        public dynamic Headers { get; set; }
        

        public void PrepareExportCommand(SqlCommand command)
        {
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.AddWithValue("@To", EndPoint.ToString());            
            
            var headers = (ICollection<KeyValuePair<string, object>>)Headers;
            foreach (var prm in headers)
            {
                command.Parameters.AddWithValue('@' + prm.Key, prm.Value ?? DBNull.Value);
            };
            
            var messageId = command.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier);
            messageId.Direction = ParameterDirection.InputOutput;
            messageId.Value = ConvertToMd5HashGUID(string.Join("@", headers.Select(h => h.Value)));
        }

        public static Guid ConvertToMd5HashGUID(string value)
        {
            // convert null to empty string - null can not be hashed
            if (value == null)
                value = string.Empty;

            // get the byte representation
            var bytes = Encoding.Default.GetBytes(value);

            // create the md5 hash
            MD5 md5Hasher = MD5.Create();
            byte[] data = md5Hasher.ComputeHash(bytes);

            // convert the hash to a Guid
            return new Guid(data);
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
            dataReader.Close();
            return result.TryParse();            
        }

        Envelope TryParse()
        {
            try
            {
                if (EndPoint.Scheme != "mssql")
                    throw new Exception("Not supported endpoint !");
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
                else if (name == "DeliveryDate")
                    envelope.DeliveryDate = await dataReader.GetFieldValueAsync<DateTime>(i).ConfigureAwait(false);
                else if (name == "Error")
                    envelope.Error = await GetNullableAsync<string>(dataReader, i).ConfigureAwait(false);
                else if (name == "RowVersion")
                    envelope.RowVersion = await dataReader.GetFieldValueAsync<long>(i).ConfigureAwait(false);
                else
                    headers.Add(new KeyValuePair<string, object>(name, await GetNullableAsync<object>(dataReader, i).ConfigureAwait(false)));
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
