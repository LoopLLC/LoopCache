using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoopCache.Client
{
    public class Response
    {
        public enum Types : byte
        {
            InvalidRequestType = 1,
            NotMasterNode = 2,
            NotDataNode = 3,
            ObjectOk = 4,
            ObjectMissing = 5,
            ReConfigure = 6,
            Configuration = 7,
            InternalServerError = 8,
            ReadKeyError = 9,
            ReadDataError = 10,
            UnknownNode = 11,
            EndPointMismatch = 12,
            NodeExists = 13,
            Accepted = 14,
            DataNodeNotReady = 15
        }

        public Response(Types type, byte[] data)
        {
            this.Type = type;
            this.Data = data;
        }

        public Types Type { get; set; }
        public byte[] Data { get; set; }
    }
}
