using System;
using AWSGsrSerDe.serializer.avro;

namespace AWSGsrSerDe.serializer
{
    public class SerializerFactory
    {
        private AvroSerializer _avroSerializer;

        public IDataFormatSerializer getSerializer(string dataFormat)
        {
            switch (dataFormat)
            {
                case "AVRO":
                    return getAvroSerializer();
                default:
                    throw new Exception($"Unsupported data format: {dataFormat}");
            }
        }


        private AvroSerializer getAvroSerializer()
        {
            if (_avroSerializer == null)
            {
                _avroSerializer = new AvroSerializer();
            }

            return _avroSerializer;
        }
    }
}