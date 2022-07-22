using System;
using AWSGsrSerDe.serializer.avro;

namespace AWSGsrSerDe.serializer
{
    public class SerializerFactory
    {
        private static SerializerFactory _serializerFactoryInstance;
        
        private AvroSerializer _avroSerializer;

        private SerializerFactory()
        {
        }

        public static SerializerFactory GetInstance()
        {
            return _serializerFactoryInstance ??= new SerializerFactory();
        }

        public IDataFormatSerializer GetSerializer(string dataFormat)
        {
            return dataFormat switch
            {
                "AVRO" => GetAvroSerializer(),
                _ => throw new AwsSchemaRegistryException($"Unsupported data format: {dataFormat}")
            };
        }
        
        private AvroSerializer GetAvroSerializer()
        {
            return _avroSerializer ??= new AvroSerializer();
        }
    }
}
