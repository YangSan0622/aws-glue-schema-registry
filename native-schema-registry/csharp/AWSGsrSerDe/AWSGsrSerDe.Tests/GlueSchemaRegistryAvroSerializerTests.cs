using System;
using System.Collections.Generic;
using System.IO;
using Avro;
using Avro.Generic;
using Avro.IO;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.serializer.avro;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests
{
    [TestFixture]
    public class GlueSchemaRegistryAvroSerializerTests
    {
        private const string TestAvroSchema = "{\"namespace\": \"example.avro\",\n"
                                              + " \"type\": \"record\",\n"
                                              + " \"name\": \"User\",\n"
                                              + " \"fields\": [\n"
                                              + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                                              + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
                                              + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
                                              + " ]\n"
                                              + "}";

        private AvroSerializer _avroSerializer = new AvroSerializer();

        [Test]
        public void KafkaSerializerTest()
        {
            var avroRecord = GetTestAvroRecord();
            
            GlueSchemaRegistryKafkaSerializer kafkaSerializer = new GlueSchemaRegistryKafkaSerializer();
            var serialize = kafkaSerializer.Serialize(avroRecord, "test-topic");
            Console.WriteLine(BitConverter.ToString(serialize));
        }
        
        [Test]
        public void KafkaDeserializerTest()
        {
            var avroRecord = GetTestAvroRecord();
            var configs = new Dictionary<string, dynamic>();
            configs.Add(GlueSchemaRegistryConstants.AvroRecordType, AvroRecordType.GenericRecord);

            var kafkaSerializer = new GlueSchemaRegistryKafkaSerializer();
            var kafkaDeserializer = new GlueSchemaRegistryKafkaDeserializer(configs);
            

            var bytes = kafkaSerializer.Serialize(avroRecord, "test-topic");
            var deserializeObject = kafkaDeserializer.Deserialize("test-topic", bytes);
            
            Assert.IsTrue(deserializeObject is GenericRecord);
            var genericRecord = (GenericRecord)deserializeObject;
            
            Assert.AreEqual(avroRecord,genericRecord);
        }
        
        [Test]
        public void WhenSerializeIsCalledReturnsCachedInstance()
        {
            var genericRecord = GetTestAvroRecord();

            var user = new User();
            user.Put(0, "stray");
            user.Put(1, 2022);
            user.Put(2,"orange");

            _avroSerializer.Serialize(genericRecord);
            _avroSerializer.Serialize(user);

            Assert.AreEqual(2, _avroSerializer.CacheSize());
        }
        
        
        

        private static GenericRecord GetTestAvroRecord()
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var user = new GenericRecord((RecordSchema)recordSchema);

            // user.Add ("name", "Alyssa🌯 🫔 🥗 🥘 🫕 🥫 🍝 🍜 🍲 🍛 🍣 🍱 🥟 🦪 🍤 🍙 🍚 🍘 🍥");
            // user.Add("favorite_number", 256);
            // user.Add("favorite_color", "blue");
            user.Add("name","sansa");
            user.Add("favorite_number", 99);
            user.Add("favorite_color", "red");
            return user;
        }
        private static byte[] GetAvroMessage()
        {
            var user = GetTestAvroRecord();
        
            var genericDatumWriter = new  GenericDatumWriter<GenericRecord>(user.Schema);
            var encoded = new MemoryStream();
            var encoder = new BinaryEncoder(encoded);
            genericDatumWriter.Write(user, encoder);
            encoder.Flush();
        
            return encoded.ToArray();
        }
        
        private static GenericRecord DecodeAvroMessage(String writerSchemaText, byte[] bytes)
        {
            var recordSchema = Schema.Parse(TestAvroSchema);
            var writerSchema = Schema.Parse(writerSchemaText);
            var genericDatumReader = new GenericDatumReader<GenericRecord>(writerSchema, recordSchema);
            var decoded = new MemoryStream(bytes);
            var decoder = new BinaryDecoder(decoded);
            var decodedUser = genericDatumReader.Read(null, decoder);

            return decodedUser;
        }
    }
}