namespace AWSGsrSerDe.serializer
{
    public interface IDataFormatSerializer
    {
        byte[] Serialize(object data);

        string GetSchemaDefinition(object data);

        void Validate(string schemaDefinition, byte[] data);

        void Validate(object data);
    }
}
