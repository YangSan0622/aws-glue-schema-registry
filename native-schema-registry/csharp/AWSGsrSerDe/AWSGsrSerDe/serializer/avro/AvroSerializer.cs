using System;
using System.IO;
using System.Runtime.Caching;
using Avro;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;

namespace AWSGsrSerDe.serializer.avro
{
    public class AvroSerializer : IDataFormatSerializer
    {

        private readonly MemoryCache _datumWriterCache = new MemoryCache("datum_writer_cache");
        private readonly CacheItemPolicy _cacheItemPolicy = new CacheItemPolicy();

        public AvroSerializer()
        {
            // cache item will expire after 30 mins without getting accessed
            _cacheItemPolicy.SlidingExpiration = TimeSpan.FromSeconds(1800);
            
        }

        public byte[] Serialize(object data)
        {
            return Serialize(data, GetOrCreateDatumWriter(data));
        }

        public string GetSchemaDefinition(object data)
        {
            var schema = getSchema(data);
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


        public long CacheSize()
        {
            return _datumWriterCache.GetCount();
        }

        private byte[] Serialize(object data, DatumWriter<object> datumWriter)
        {
            var memoryStream = new MemoryStream();
            Encoder encoder = new BinaryEncoder(memoryStream);
            datumWriter.Write(data, encoder);
            return memoryStream.ToArray();
        }

        private DatumWriter<object> GetOrCreateDatumWriter(object data)
        {
            var schema = getSchema(data);
            var datumWriter = _datumWriterCache.AddOrGetExisting(schema.ToString(), 
                NewDatumWriter(data, schema), 
                _cacheItemPolicy);
            return datumWriter == null ? 
                (DatumWriter<object>)_datumWriterCache.Get(schema.ToString()) : 
                (DatumWriter<object>)datumWriter;
        }

        private Schema getSchema(object data)
        {
            return data switch
            {
                GenericRecord record => record.Schema,
                ISpecificRecord record => record.Schema,
                _ => throw new Exception("Unsupported Avro Data formats")
            };
        }

        private DatumWriter<object> NewDatumWriter(object data, Schema schema)
        {
            return data switch
            {
                ISpecificRecord _ => new SpecificDatumWriter<object>(schema),
                GenericRecord _ => new GenericWriter<object>(schema),
                _ => throw new AwsSchemaRegistryException($"Unsupported type passed for serialization: {data}")
            };
        }
    }
}