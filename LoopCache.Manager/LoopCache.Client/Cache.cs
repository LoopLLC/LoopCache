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
    public class Cache
    {
        public MasterNode Master;

        /// <summary>
        /// Construct a new instance based on the master node's hostname:port
        /// </summary>
        /// <param name="masterHostNamePort">The master node</param>
        public Cache(string hostName, int port)
        {
            this.Master = new MasterNode(hostName, port);
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

            var response = this.SendMessage(request);
            if (response != null)
            {
                if (response.Type == Response.Types.ObjectOk)
                    returnValue = Common.FromByteArray<T>(response.Data);
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
                byte[] dataBytes = Common.ToByteArray(value);
                w.Write(IPAddress.HostToNetworkOrder(dataBytes.Length));
                w.Write(dataBytes);
                w.Flush();
                ms.Flush();
                data = ms.ToArray();
            }

            Request request = new Request(Request.Types.SetObject, key, data);
            var response = this.SendMessage(request);

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
            var response = this.SendMessage(request);

            if (response == null)
                return false;

            return (response.Type == Response.Types.ObjectOk);
        }

        public bool Clear()
        {
            return this.Master.Clear(); 
        }

        /// <summary>
        /// This method will try 3 times to connect with the node, 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="request"></param>
        /// <returns></returns>
        private Response SendMessage(Request request)
        {
            Response response = null;
            //
            //  We'll try 3 times to get our data.
            //  Each node can respond with "im not the right node".
            //  If this happens the node responds with new configuration.
            //  Retry with the new configuration.
            //
            Node node;

            for (int i = 0; i < 3; i++)
            {
                node = this.Master.GetNodeForKey(request.Key);
                response = Common.SendMessage(node.HostName, node.Port, request);

                if (response == null)
                {
                    //
                    //  Distination unreachable.
                    //      Tell the master the node is down.
                    this.Master.NodeUnreachable(node.HostName, node.Port);
                    //
                    //  Since the node didnt respond it cant give us a new config. 
                    //  Go get it from master.
                    //
                    this.Master.GetConfig();
                    continue;
                }

                switch (response.Type)
                {
                    case Response.Types.ObjectOk:
                        break;

                    case Response.Types.ReConfigure:                    
                        this.Master.ReadConfigBytes(response.Data);
                        continue;

//                    case Response.Types.DataNodeNotReady:
//                        this.Master.Status(
                        //continue;

                    default:
                        string ex = string.Format(
                            "Unexpected {0} response for request {1}.",
                            response.Type,
                            request.Data
                        );
                        throw new Exception(ex);
                }

                break;
            }

            return response;
        }

        public class MasterNode : Node
        {
            public MasterNode(string hostName, int port) : base(hostName, port)
            {
                this.RingNodes = new SortedList<int, Node>();
                this.Nodes = new SortedList<string, Node>();

                if (this.RingNodes.Count == 0)
                    this.GetConfig();
            }

            /// <summary>
            /// A map of the virtual node locations
            /// </summary>
            public SortedList<int, Node> RingNodes;

            /// <summary>
            /// A list of "host:port" => Node
            /// </summary>
            public SortedList<string, Node> Nodes;

            /// <summary>
            /// Add a node to the cluster.
            /// </summary>
            /// <remarks>The node needs to be up and running already.</remarks>
            /// <param name="hostPort">hostname:port for the new node</param>
            /// <param name="maxNumBytes">Max number of bytes the new node can handle</param>
            /// <returns>true if the message was delivered to master successfully, but
            /// the master does some work in the background after returning, so 
            /// it's possible for something to fail afterwards.</returns>
            public bool AddNode(string hostname, int port, long maxNumBytes)
            {
                //     Request Layout:

                //          HostLen         int
                //          Host            byte[] UTF8 string
                //          Port            int
                //          MaxNumBytes     long
                //          Status          byte

                byte[] data = null;
                using (MemoryStream ms = new MemoryStream())
                {
                    BinaryWriter w = new BinaryWriter(ms);

                    byte[] hostBytes = Common.ToByteArray(hostname);
                    w.Write(IPAddress.HostToNetworkOrder(hostBytes.Length));
                    w.Write(hostBytes);
                    w.Write(IPAddress.HostToNetworkOrder(port));
                    w.Write(IPAddress.HostToNetworkOrder(maxNumBytes));
                    w.Write((byte)Node.StatusType.Up);
                    w.Write((byte)0);

                    w.Flush();
                    ms.Flush();
                    data = ms.ToArray();
                }

                Request request = new Request(Request.Types.AddNode, data);
                var response = this.SendMessage(request);

                if (response == null)
                    return false;

                return (response.Type == Response.Types.Accepted);
            }

            /// <summary>
            /// Remove a node from the cluster.
            /// </summary>
            /// <param name="hostPort"></param>
            /// <returns></returns>
            public bool RemoveNode(string hostname, int port)
            {
                //     Request Layout:

                //          HostLen         int
                //          Host            byte[] UTF8 string
                //          Port            int

                byte[] data = null;
                using (MemoryStream ms = new MemoryStream())
                {
                    BinaryWriter w = new BinaryWriter(ms);

                    byte[] hostBytes = Common.ToByteArray(hostname);
                    w.Write(IPAddress.HostToNetworkOrder(hostBytes.Length));
                    w.Write(hostBytes);
                    w.Write(IPAddress.HostToNetworkOrder(port));

                    w.Flush();
                    ms.Flush();
                    data = ms.ToArray();
                }

                Request request = new Request(Request.Types.RemoveNode, data);
                var response = this.SendMessage(request);

                if (response == null)
                    return false;

                return (response.Type != Response.Types.Accepted);
            }

            public Response NodeUnreachable(string hostname, int port)
            {
                //  MessageType     byte (2)
                //  DataLength      int
                //  Data            byte[]
                //      HostLen         int
                //      Host            byte[] UTF8 string
                //      Port            int

                byte[] data = null;
                using (MemoryStream ms = new MemoryStream())
                {
                    BinaryWriter w = new BinaryWriter(ms);

                    byte[] hostBytes = Common.ToByteArray(hostname);
                    w.Write(IPAddress.HostToNetworkOrder(hostBytes.Length));
                    w.Write(hostBytes);
                    w.Write(IPAddress.HostToNetworkOrder(port));

                    w.Flush();
                    ms.Flush();
                    data = ms.ToArray();
                }

                Request request = new Request(Request.Types.NodeUnreachable, data);
                return this.SendMessage(request);
            }

            private Response SendMessage(Request request)
            {
                return Common.SendMessage(this.HostName, this.Port, request);
            }

            /// <summary>
            /// Reload the ring configuration from master.
            /// </summary>
            /// <returns></returns>
            public void GetConfig()
            {
                Request request = new Request(Request.Types.GetConfig);

                var response = this.SendMessage(request);

                if (response != null)
                {
                    if (response.Type == Response.Types.Configuration)
                        this.ReadConfigBytes(response.Data);

                    Common.EndPoints.Clear();

                    //if (this.Nodes.Count == 0)
                    //throw new Exception("Contacted master and received 0 data nodes.");
                }
            }

            public void ReadConfigBytes(byte[] data)
            {
                this.RingNodes = new SortedList<int, Node>();
                this.Nodes = new SortedList<string, Node>();

                if (data.Length > 0)
                {
                    using (MemoryStream ms = new MemoryStream(data))
                    {
                        using (BinaryReader reader = new BinaryReader(ms))
                        {
                            int numNodes = Common.Read(reader.ReadInt32());
                            for (int n = 0; n < numNodes; n++)
                            {
                                int hostLen = Common.Read(reader.ReadInt32());
                                byte[] hostData = new byte[hostLen];
                                if (reader.Read(hostData, 0, hostLen) != hostLen)
                                    throw new Exception("Invalid GetConfig hostData");

                                string hostname = Common.Read(hostData);
                                int port = Common.Read(reader.ReadInt32());
                                long maxNumBytes = Common.Read(reader.ReadInt64());

                                Node node = new Node(
                                    hostname,
                                    port,
                                    maxNumBytes,
                                    (Node.StatusType)reader.ReadByte()
                                );

                                this.Nodes.Add(node.Name, node);

                                bool parseLocations = reader.ReadBoolean();

                                if (parseLocations)
                                {
                                    int numLocations = Common.Read(reader.ReadInt32());
                                    for (int i = 0; i < numLocations; i++)
                                    {
                                        int location = Common.Read(reader.ReadInt32());

                                        if (this.RingNodes.ContainsKey(location))
                                            throw new Exception("Something");

                                        this.RingNodes.Add(location, node);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            /// <summary>
            /// Get the data node that owns this key
            /// </summary>
            /// <remarks>
            /// Virtual nodes are on a 32 bit ring.  Find the location 
            /// of the key's hash and the node that owns it is the first node
            /// we find from that point upwards.
            /// </remarks>
            public Node GetNodeForKey(string key)
            {
                Node returnValue = null;

                int hashCode = Common.GetConsistentHashCode(key);

                var keys = this.RingNodes.Keys;

                for (int i = 0; i < keys.Count; i++)
                {
                    if (keys[i] >= hashCode)
                    {
                        returnValue = this.RingNodes[keys[i]];
                        break;
                    }
                }

                if (returnValue == null)
                    returnValue = this.RingNodes[keys[0]];

                return returnValue;
            }

            public bool Clear()
            {
                Request request = new Request(Request.Types.Clear);
                var response = this.SendMessage(request);

                if (response == null)
                    return false;

                return (response.Type == Response.Types.Accepted);
            }
        }
    }
}