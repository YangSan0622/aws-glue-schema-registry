
using System;
using AWSGsrSerDe.common;

namespace AWSGsrSerDe.serializer
{
    public class GlueSchemaRegistryKafkaSerializer
    {
        private readonly IDataFormatSerializer _serializer;
        private readonly string _dataFormat;
        private readonly Func<string, object, string> _schemaNamingStrategy;


        public GlueSchemaRegistryKafkaSerializer()
        {
            // TODO: remove hardcode property with Config instead
            _dataFormat = GlueSchemaRegistryConstants.DataFormat.AVRO.ToString();
            _schemaNamingStrategy = (topic, data) => topic;
            
            _serializer = SerializerFactory.GetInstance().GetSerializer(_dataFormat);
        }

        public byte[] Serialize(object data, string topic)
        {
            if (null == data)
            {
                return null;
            }
            
            var bytes = _serializer.Serialize(data);
            var schemaDefinition = _serializer.GetSchemaDefinition(data);

            var glueSchemaRegistrySchema = new GlueSchemaRegistrySchema(_schemaNamingStrategy(topic, data), 
                schemaDefinition, 
                _dataFormat);

            var glueSchemaRegistrySerializer = new GlueSchemaRegistrySerializer();
            var encodedResult = glueSchemaRegistrySerializer.Encode(topic, glueSchemaRegistrySchema, bytes);

            return encodedResult;
        }
        
    }
}
