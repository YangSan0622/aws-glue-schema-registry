using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Reflection.Metadata;
using System.Runtime.Caching;
using Avro;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using AWSGsrSerDe.common;
using AWSGsrSerDe.serializer.avro;

namespace AWSGsrSerDe.deserializer.avro
{
    public class AvroDeserializer : IDataFormatDeserializer
    {
        private readonly MemoryCache _datumReaderCache = new MemoryCache("datum_reader_cache");
        private readonly CacheItemPolicy _cacheItemPolicy = new CacheItemPolicy();
        private AvroRecordType _avroRecordType;

        public long CacheSize => _datumReaderCache.GetCount();

        private AvroDeserializer()
        {
        }

        public AvroDeserializer(GlueSchemaRegistryConfiguration configs)
            : this()
        {
            Configure(configs);
        }

        public void Configure(GlueSchemaRegistryConfiguration configs)
        {
            _avroRecordType = configs.AvroRecordType;
            var cacheItemExpirationTime = configs.CacheItemExpirationTime;
            _cacheItemPolicy.SlidingExpiration = TimeSpan.FromSeconds(cacheItemExpirationTime);
        }


        public object Deserialize([NotNull] byte[] data, [NotNull] GlueSchemaRegistrySchema schema)
        {
            try
            {
                var schemaDefinition = schema.SchemaDef;
                Debug.WriteLine($"Length of actual message: {data.Length}");

                var datumReader = GetDatumReader(schemaDefinition);
                var memoryStream = new MemoryStream(data);
                var binaryDecoder = new BinaryDecoder(memoryStream);
                return datumReader.Read(null, binaryDecoder);
            }
            catch (Exception e)
            {
                const string message = "Exception occurred while de-serializing Avro message";
                throw new AwsSchemaRegistryException(message, e);
            }
        }

        private DatumReader<object> GetDatumReader(string schema)
        {
            var datumReader = _datumReaderCache.AddOrGetExisting(
                schema,
                NewDatumReader(schema),
                _cacheItemPolicy);
            return datumReader == null
                ? (DatumReader<object>)_datumReaderCache.Get(schema)
                : (DatumReader<object>)datumReader;
        }

        private DatumReader<object> NewDatumReader(string schema)
        {
            var schemaObject = Schema.Parse(schema);

            return _avroRecordType switch
            {
                AvroRecordType.GenericRecord => new GenericDatumReader<object>(schemaObject, schemaObject),
                AvroRecordType.SpecificRecord => new SpecificDatumReader<object>(schemaObject, schemaObject),
                _ => throw new AwsSchemaRegistryException($"Unsupported AvroRecordType: {_avroRecordType}")
            };
        }
    }
}
