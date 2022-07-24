using System.Collections.Generic;
using AWSGsrSerDe.common;

namespace AWSGsrSerDe.deserializer
{
    public interface IDataFormatDeserializer
    {
        void Configure(GlueSchemaRegistryConfiguration configs);
        object Deserialize(byte[] data, GlueSchemaRegistrySchema schema);
    }
}