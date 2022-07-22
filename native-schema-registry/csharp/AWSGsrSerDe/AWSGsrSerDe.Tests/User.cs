using Avro;
using Avro.Specific;

namespace AWSGsrSerDe.Tests
{
    public class User : ISpecificRecord
    {
        
        private static Schema _schema = Schema.Parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"AWSGsrSerDe.Tests\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}");

        private string _name;
        private int _favoriteNumber;
        private string _favoriteColor;
        

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return this._name;
                case 1: return this._favoriteNumber;
                case 2: return this._favoriteColor;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            };
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: this._name = (string)fieldValue; break;
                case 1: this._favoriteNumber = (int)fieldValue; break;
                case 2: this._favoriteColor = (string)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            };
        }

        public Schema Schema
        {
            get { return User._schema; }
        }
    }
}