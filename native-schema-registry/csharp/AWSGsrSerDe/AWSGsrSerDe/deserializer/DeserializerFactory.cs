using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer.avro;

namespace AWSGsrSerDe.deserializer
{
    public class DeserializerFactory
    {
        private static DeserializerFactory _deserializerFactoryInstance;
        private AvroDeserializer _avroDeserializer;

        public static DeserializerFactory GetInstance()
        {
            return _deserializerFactoryInstance ??= new DeserializerFactory();
        }

        private DeserializerFactory()
        {
        }

        public IDataFormatDeserializer GetDeserializer(string dataFormat, GlueSchemaRegistryConfiguration configs)
        {
            switch (dataFormat)
            {
                case "AVRO":
                    return GetAvroDeserializer(configs);
                default:
                    throw new AwsSchemaRegistryException($"Unsupported data format: {dataFormat}");
            }
        }


        private AvroDeserializer GetAvroDeserializer(GlueSchemaRegistryConfiguration configs)
        {
            if (_avroDeserializer == null)
            {
                _avroDeserializer = new AvroDeserializer(configs);
            }

            return _avroDeserializer;
        }
    }
}
