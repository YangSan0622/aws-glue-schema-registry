using System;
using System.IO;
using Avro;

namespace AWSGsrSerDe.Tests.utils
{
    public class SchemaLoader
    {
        public static Schema LoadAvroSchema(string schemaFilePath)
        {
            Schema schema = null;
            try
            {
                var schemaDefinition = File.ReadAllText(schemaFilePath);
                schema = Schema.Parse(schemaDefinition);
            }
            catch (Exception e)
            {
                throw new AwsSchemaRegistryException("Failed to parse the avro schema file", e);
            }
            return schema;
        }
    }
}
