
using System;

namespace AWSGsrSerDe.serializer
{
    public class GlueSchemaRegistryKafkaSerializer
    {
        private readonly IDataFormatSerializer _serializer;
        private readonly string _dataFormat;
        private readonly Func<string, object, string> _schemaNamingStradegy;


        public GlueSchemaRegistryKafkaSerializer()
        {
            // TODO: remove hardcode property with Config instead
            _dataFormat = "AVRO";
            _schemaNamingStradegy = (topic, data) => topic;
            
            _serializer = new SerializerFactory().getSerializer(_dataFormat);
        }

        public byte[] Serialize(object data, String topic)
        {
            if (null == data)
            {
                return null;
            }
            
            var bytes = _serializer.Serialize(data);
            var schemaDefinition = _serializer.GetSchemaDefinition(data);

            var glueSchemaRegistrySchema = new GlueSchemaRegistrySchema(_schemaNamingStradegy(topic, data), 
                schemaDefinition, 
                _dataFormat);

            var glueSchemaRegistrySerializer = new GlueSchemaRegistrySerializer();
            var encodedResult = glueSchemaRegistrySerializer.Encode(topic, glueSchemaRegistrySchema, bytes);

            return encodedResult;
        }
        
    }
}