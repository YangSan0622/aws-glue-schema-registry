using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Caching;
using Avro;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using AWSGsrSerDe.common;

namespace AWSGsrSerDe.serializer.avro
{
    public class AvroSerializer : IDataFormatSerializer
    {

        private readonly MemoryCache _datumWriterCache = new MemoryCache("datum_writer_cache");
        private readonly CacheItemPolicy _cacheItemPolicy = new CacheItemPolicy();

        public long CacheSize => _datumWriterCache.GetCount();

        private AvroSerializer()
        {
        }

        public AvroSerializer(GlueSchemaRegistryConfiguration configuration = null)
        :this()
        {
            configuration ??= new GlueSchemaRegistryConfiguration(new Dictionary<string, dynamic>());
            _cacheItemPolicy.SlidingExpiration = TimeSpan.FromSeconds(configuration.CacheItemExpirationTime);
        }

        public byte[] Serialize(object data)
        {
            return data==null?
                null:
                Serialize(data, GetOrCreateDatumWriter(data));
        }

        public string GetSchemaDefinition(object data)
        {
            var schema = GetSchema(data);
            return schema.ToString();
        }

        public void Validate(string schemaDefinition, byte[] data)
        {
            //No-op
            //We cannot determine accurately if the data bytes match the schema as Avro bytes don't contain the field names.
        }

        public void Validate(object data)
        {
            //No-op
            //Avro format assumes that the passed object contains schema and data that are mutually conformant.
            //We cannot validate the data against the schema.
        }

        private static byte[] Serialize(object data, DatumWriter<object> datumWriter)
        {
            var memoryStream = new MemoryStream();
            Encoder encoder = new BinaryEncoder(memoryStream);
            datumWriter.Write(data, encoder);
            return memoryStream.ToArray();
        }

        private DatumWriter<object> GetOrCreateDatumWriter(object data)
        {
            var schema = GetSchema(data);
            var dataType = data.GetType().ToString();
            var cacheKey = schema + dataType;
            var datumWriter = _datumWriterCache.AddOrGetExisting(cacheKey, 
                NewDatumWriter(data, schema), 
                _cacheItemPolicy);
            return datumWriter == null ? 
                (DatumWriter<object>)_datumWriterCache.Get(cacheKey) : 
                (DatumWriter<object>)datumWriter;
        }

        private static Schema GetSchema(object data)
        {
            return data switch
            {
                GenericRecord record => record.Schema,
                GenericEnum record => record.Schema,
                ISpecificRecord record => record.Schema,
                GenericFixed record => record.Schema,
                _ => throw new AwsSchemaRegistryException("Unsupported Avro Data formats")
            };
        }

        private static DatumWriter<object> NewDatumWriter(object data, Schema schema)
        {
            return data switch
            {
                ISpecificRecord _ => new SpecificDatumWriter<object>(schema),
                GenericRecord _ => new GenericWriter<object>(schema),
                GenericEnum _ => new GenericWriter<object>(schema),
                GenericFixed _=> new GenericWriter<object>(schema),
                _ => throw new AwsSchemaRegistryException($"Unsupported type passed for serialization: {data}")
            };
        }
    }
}
