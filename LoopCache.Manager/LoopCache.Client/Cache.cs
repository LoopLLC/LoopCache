using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;

namespace LoopCache.Client
{
    /// <summary>
    /// LoopCache Client.
    /// </summary>
    public class Cache : CacheBase
    {
        /// <summary>
        /// Construct a new instance based on the master node's hostname:port
        /// </summary>
        /// <param name="masterHostName">The master HostName</param>
        /// <param name="masterPort">The master Port</param>
        public Cache(string masterHostName, int masterPort) : base(masterHostName, masterPort)
        {
        }

        /// <summary>
        /// Gets or sets an item in the cache.
        /// </summary>
        public object this[string key]
        {
            get { return this.Get(key); }
            set { this.Set(key, value); }
        }

        /// <summary>
        /// Retrieves an item from the cache.
        /// </summary>
        public object Get(string key)
        {
            return this.Get<object>(key);
        }

        /// <summary>
        /// Retrieves an item from the cache.
        /// </summary>
        public T Get<T>(string key)
        {
            T returnValue = default(T);

            Request request = new Request(Request.Types.GetObject, key, Encoding.UTF8.GetBytes(key));

            var response = base.SendNodeRequest(request);
            if (response != null)
            {
                if (response.Type == Response.Types.ObjectOk)
                    returnValue = CacheBase.FromByteArray<T>(response.Data);
            }

            return returnValue;
        }

        /// <summary>
        /// Inserts or Updates an item into the Cache.
        /// </summary>
        public bool Set(string key, object value)
        {
            byte[] data = null;
            using (MemoryStream ms = new MemoryStream())
            {
                BinaryWriter w = new BinaryWriter(ms);
                byte[] keyBytes = Encoding.UTF8.GetBytes(key);
                w.Write(IPAddress.HostToNetworkOrder(keyBytes.Length));
                w.Write(keyBytes);
                byte[] dataBytes = CacheBase.ToByteArray(value);
                w.Write(IPAddress.HostToNetworkOrder(dataBytes.Length));
                w.Write(dataBytes);
                w.Flush();
                ms.Flush();
                data = ms.ToArray();
            }

            Request request = new Request(Request.Types.SetObject, key, data);
            var response = base.SendNodeRequest(request);

            if (response == null)
                return false;

            return (response.Type == Response.Types.ObjectOk);
        }

        /// <summary>
        /// Removes an item from the Cache.
        /// </summary>
        public bool Remove(string key)
        {
            Request request = new Request(Request.Types.DeleteObject, key, Encoding.UTF8.GetBytes(key));
            var response = base.SendNodeRequest(request);

            if (response == null)
                return false;

            return (response.Type == Response.Types.ObjectOk);
        }
    }
}