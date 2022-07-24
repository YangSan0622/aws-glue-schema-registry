using System.Collections.Generic;

namespace AWSGsrSerDe.common
{
    public class GlueSchemaRegistryConfiguration
    {
        private AvroRecordType _avroRecordType;
        public AvroRecordType AvroRecordType => _avroRecordType;

        public GlueSchemaRegistryConfiguration(Dictionary<string, dynamic> configs)
        {
            BuildConfigs(configs);
        }

        private void BuildConfigs(Dictionary<string, dynamic> configs)
        {
            ValidateAndSetAvroRecordType(configs);
        }

        private void ValidateAndSetAvroRecordType(Dictionary<string, dynamic> configs)
        {
            if (configs.ContainsKey(GlueSchemaRegistryConstants.AvroRecordType))
            {
                _avroRecordType = configs[GlueSchemaRegistryConstants.AvroRecordType];
            }
        }
    }
}