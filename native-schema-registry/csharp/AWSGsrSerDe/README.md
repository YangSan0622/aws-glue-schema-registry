# AWS Glue Schema Registry Serializers / De-serializers for C#

This package provides the Glue Schema Registry (GSR) serializers / de-serializers for Avro, JSON and Protobuf data formats.


## Development
The C# serializers / de-serializers (SerDes) are built as bindings over existing C library of GSR SerDes. The C library is a facade over GraalVM compiled Java SerDes.

### Building
#### Building the C / Java code
Follow the instructions in those specific projects to build them.

#### Building C# code

```
dotnet clean .
dotnet build .
dotnet test .
```


### Testing

#### Link the test resources
In native-schema-registry/csharp/AWSGsrSerDe/AWSGsrSerDe.Tests/resources directory, run
```shell
ln -s ../../../../../serializer-deserializer/src/test/resources/avro avro &&
ln -s ../../../../../serializer-deserializer/src/test/resources/json json &&
ln -s ../../../../../serializer-deserializer/src/test/resources/protobuf protobuf 
```
