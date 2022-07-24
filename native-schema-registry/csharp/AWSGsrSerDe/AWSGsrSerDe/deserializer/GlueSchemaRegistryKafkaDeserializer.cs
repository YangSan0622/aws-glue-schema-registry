using System.Collections.Generic;
using AWSGsrSerDe.common;

namespace AWSGsrSerDe.deserializer
{
    public class GlueSchemaRegistryKafkaDeserializer
    {
        private readonly DeserializerFactory _deserializerFactory = new DeserializerFactory();
        private readonly GlueSchemaRegistryConfiguration _configuration;

        public GlueSchemaRegistryKafkaDeserializer(Dictionary<string, dynamic> configs)
        {
            _configuration = new GlueSchemaRegistryConfiguration(configs);
        }
        
        public object Deserialize(string topic, byte[] data)
        {
            if (null == data)
            {
                return null;
            }

            var glueSchemaRegistryDeserializer = new GlueSchemaRegistryDeserializer();
            var decodedBytes = glueSchemaRegistryDeserializer.Decode(data);
            var schemaRegistrySchema = glueSchemaRegistryDeserializer.DecodeSchema(data);

            var dataFormat = schemaRegistrySchema.DataFormat;
            var deserializer = _deserializerFactory.GetDeserializer(dataFormat,_configuration);

            var result = deserializer.Deserialize(decodedBytes, schemaRegistrySchema);

            return result;
        }
    }
}