using System;
using System.Collections.Generic;
using System.IO;
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

        public AvroDeserializer()
        {
            // cache item will expire after 30 mins without getting accessed
            _cacheItemPolicy.SlidingExpiration = TimeSpan.FromSeconds(1800);
        }

        public AvroDeserializer(GlueSchemaRegistryConfiguration configs)
            : this()
        {
            Configure(configs);
        }

        public void Configure(GlueSchemaRegistryConfiguration configs)
        {
            _avroRecordType = configs.AvroRecordType;
        }


        public object Deserialize(byte[] data, GlueSchemaRegistrySchema schema)
        {
            var schemaDefinition = schema.SchemaDef;
            Console.WriteLine($"Length of actual message: {data.Length}");

            var datumReader = GetDatumReader(schemaDefinition);
            var memoryStream = new MemoryStream(data);
            var binaryDecoder = new BinaryDecoder(memoryStream);
            return datumReader.Read(null, binaryDecoder);
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
            switch (_avroRecordType)
            {
                case AvroRecordType.GenericRecord:
                    return new GenericDatumReader<object>(schemaObject, schemaObject);
                case AvroRecordType.SpecificRecord:
                    return new SpecificDatumReader<object>(schemaObject, schemaObject);
                default:
                    throw new AwsSchemaRegistryException($"Unsupported AvroRecordType:{_avroRecordType}");
            }
        }
    }
}