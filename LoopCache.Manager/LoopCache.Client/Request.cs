using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoopCache.Client
{
    public class Request
    {
        public enum Types : byte
        {
            GetConfig = 1,
            NodeUnreachable = 2,
            AddNode = 3,
            RemoveNode = 4,
            ChangeNode = 5,
            GetStats = 6,
            GetObject = 7,
            SetObject = 8,
            DeleteObject = 9,
            ChangeConfig = 10,
            Register = 11,
            Ping = 12,
            FireSale = 13,
            Clear = 14
        }

        public Request(Types type)
        {
            this.Key = null;
            this.Type = type;
            this.Data = null;
        }

        public Request(Types type, byte[] data)
        {
            this.Key = null;
            this.Type = type;
            this.Data = data;
        }

        public Request(Types type, string key, byte[] data)
        {
            this.Key = key;
            this.Type = type;
            this.Data = data;
        }

        public string Key { get; set; }
        public Types Type { get; set; }
        public byte[] Data { get; set; }
    }
}
