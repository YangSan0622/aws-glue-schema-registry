using System.Collections.Generic;

namespace AWSGsrSerDe.common
{
    public class GlueSchemaRegistryConfiguration
    {
        public AvroRecordType AvroRecordType { get; private set; }
        public int CacheItemExpirationTime { get; private set; } = 1800;

        public GlueSchemaRegistryConfiguration(Dictionary<string, dynamic> configs)
        {
            BuildConfigs(configs);
        }

        private void BuildConfigs(Dictionary<string, dynamic> configs)
        {
            ValidateAndSetAvroRecordType(configs);
            ValidateAndSetCacheItemExpirationTime(configs);
        }

        private void ValidateAndSetAvroRecordType(Dictionary<string, dynamic> configs)
        {
            if (configs.ContainsKey(GlueSchemaRegistryConstants.AvroRecordType))
            {
                AvroRecordType = configs[GlueSchemaRegistryConstants.AvroRecordType];
            }
        }

        private void ValidateAndSetCacheItemExpirationTime(Dictionary<string, dynamic> configs)
        {
            if (configs.ContainsKey(GlueSchemaRegistryConstants.CacheItemExpirationTime))
            {
                CacheItemExpirationTime = configs[GlueSchemaRegistryConstants.CacheItemExpirationTime];
            }
        }
    }
}
