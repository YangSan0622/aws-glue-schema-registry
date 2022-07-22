namespace AWSGsrSerDe.common
{
    public class GlueSchemaRegistryConstants
    {
        public const string AvroRecordType = "avroRecordType";
        public const string CacheItemExpirationTime = "cacheItemExpirationTime";

        public enum DataFormat
        {
            UNKNOWN,
            AVRO,
            JSON,
            PROTOBUF
        }
    }
}
