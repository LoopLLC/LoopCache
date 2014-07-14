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
    /// LoopCache support methods and properties.
    /// </summary>
    public class CacheBase
    {
        #region Instance Stuff

        protected string MasterHostName { get; set; }
        protected int MasterPort { get; set; }

        public CacheBase(string masterHostName, int masterPort)
        {
            this.MasterHostName = masterHostName;
            this.MasterPort = masterPort;
        }

        /// <summary>
        /// This method will try 3 times to connect with the node, 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="request"></param>
        /// <returns></returns>
        protected Response SendNodeRequest(Request request)
        {
            Response response = null;
            //
            //  We'll try 3 times to get our data.
            //  Each node can respond with "im not the right node".
            //  If this happens the node responds with new configuration.
            //  Retry with the new configuration.
            //
            Node node = CacheBase.GetNodeForKey(request.Key);
            bool success = false;

            for (int i = 0; i < 3; i++)
            {
                response = this.SendRequest(node.HostName, node.Port, request);

                if (response == null)
                {
                    //
                    //  Distination unreachable.
                    //      Ask the master if the node is really down.
                    response = this.NodeUnreachable(node.HostName, node.Port);

                    switch (response.Type)
                    {
                        case Response.Types.Accepted:
                            //
                            //  The node is in fact gone. BOOM!
                            //
                            break;

                        case Response.Types.NodeExists:
                            //
                            //  Master says the node is fine.
                            //  Wait a tick and try again.
                            //
                            System.Threading.Thread.Sleep(50);
                            continue;

                        default:
                            string ex = string.Format(
                                "Unexpected {0} response for request {1}.",
                                response.Type,
                                request.Data
                            );
                            throw new Exception(ex);
                    }
                }
                else
                {
                    switch (response.Type)
                    {
                        case Response.Types.ObjectOk:
                            success = true;
                            break;

                        case Response.Types.ReConfigure:
                            this.ReadConfigBytes(response.Data);
                            node = CacheBase.GetNodeForKey(request.Key);
                            continue;

                        case Response.Types.DataNodeNotReady:
                            //
                            //  Node isn't ready.
                            //  Wait a tick and try again.
                            //
                            System.Threading.Thread.Sleep(50);
                            continue;

                        default:
                            string ex = string.Format(
                                "Unexpected {0} response for request {1}.",
                                response.Type,
                                request.Data
                            );
                            throw new Exception(ex);
                    }
                }

                break;
            }

            if (!success)
                throw new Exception("Cache Node is not available.");

            return response;
        }

        protected Response SendRequest(string hostname, int port, Request request)
        {
            IPEndPoint ipep = CacheBase.GetIPEndPoint(hostname, port);

            if (ipep != null)
            {
                using (TcpClient client = new TcpClient())
                {
                    try
                    {
                        client.Connect(ipep);

                        using (NetworkStream stream = client.GetStream())
                        {
                            this.WriteRequest(stream, request);
                            return this.ReadResponse(stream);
                        }
                    }
                    catch (SocketException)
                    {
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Reload the ring configuration from master.
        /// </summary>
        /// <returns></returns>
        public void GetConfig()
        {
            Request request = new Request(Request.Types.GetConfig);

            var response = this.SendRequest(this.MasterHostName, this.MasterPort, request);

            if (response != null)
            {
                if (response.Type == Response.Types.Configuration)
                    this.ReadConfigBytes(response.Data);
            }
        }

        private void ReadConfigBytes(byte[] data)
        {
            CacheBase.RingNodes = new SortedList<int, Node>();
            CacheBase.Nodes = new SortedList<string, Node>();

            if (data.Length > 0)
            {
                using (MemoryStream ms = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(ms))
                    {
                        int numNodes = CacheBase.Read(reader.ReadInt32());
                        for (int n = 0; n < numNodes; n++)
                        {
                            int hostLen = CacheBase.Read(reader.ReadInt32());
                            byte[] hostData = new byte[hostLen];
                            if (reader.Read(hostData, 0, hostLen) != hostLen)
                                throw new Exception("Invalid GetConfig hostData");

                            string hostname = CacheBase.Read(hostData);
                            int port = CacheBase.Read(reader.ReadInt32());
                            long maxNumBytes = CacheBase.Read(reader.ReadInt64());

                            Node node = new Node(
                                hostname,
                                port,
                                maxNumBytes,
                                (Node.StatusType)reader.ReadByte()
                            );

                            CacheBase.Nodes.Add(node.Name, node);

                            bool parseLocations = reader.ReadBoolean();

                            if (parseLocations)
                            {
                                try
                                {
                                    int numLocations = CacheBase.Read(reader.ReadInt32());
                                    int location;
                                    for (int i = 0; i < numLocations; i++)
                                    {
                                        location = CacheBase.Read(reader.ReadInt32());

                                        if (!CacheBase.RingNodes.ContainsKey(location))
                                            CacheBase.RingNodes.Add(location, node);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    string s = ex.Message;
                                }
                            }
                        }
                    }
                }
            }
        }

        private void WriteRequest(NetworkStream stream, Request request)
        {
            BinaryWriter w = new BinaryWriter(stream);
            w.Write((byte)request.Type);
            if (request.Data == null)
            {
                w.Write(IPAddress.HostToNetworkOrder((int)0));
            }
            else
            {
                w.Write(IPAddress.HostToNetworkOrder(request.Data.Length));
                w.Write(request.Data);
            }
            w.Flush();
        }

        private Response ReadResponse(NetworkStream stream)
        {
            BinaryReader r = new BinaryReader(stream);
            Response.Types responseType = (Response.Types)r.ReadByte();
            int responseLength = IPAddress.NetworkToHostOrder(r.ReadInt32());
            if (responseLength > MaxLength)
            {
                throw new Exception(
                    "SendMessage got a response that exceeded max length");
            }
            byte[] responseData = new byte[0];
            if (responseLength > 0)
            {
                responseData = r.ReadBytes(responseLength);
            }

            return new Response(responseType, responseData);
        }

        private Response NodeUnreachable(string hostname, int port)
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

                byte[] hostBytes = CacheBase.ToByteArray(hostname);
                w.Write(IPAddress.HostToNetworkOrder(hostBytes.Length));
                w.Write(hostBytes);
                w.Write(IPAddress.HostToNetworkOrder(port));

                w.Flush();
                ms.Flush();
                data = ms.ToArray();
            }

            Request request = new Request(Request.Types.NodeUnreachable, data);
            return this.SendRequest( this.MasterHostName, this.MasterPort, request);
        }

        #endregion

        #region Static Stuff

        /// <summary>
        /// A map of the virtual node locations
        /// </summary>
        protected static SortedList<int, Node> RingNodes;

        /// <summary>
        /// A list of "host:port" => Node
        /// </summary>
        protected static SortedList<string, Node> Nodes;

        private static int MaxLength = 1024 * 1024; // 1Mb
        private static Dictionary<string, int> hashCodes = new Dictionary<string, int>();

        /// <summary>
        /// Convert the string to an integer representation of a consistent md5 hash.
        /// </summary>
        /// <remarks>Collisions are possible, but they don't matter because we only 
        /// want an even distribution across the range of ints for a random 
        /// assortment of strings.  This method returns the same int for the same string 
        /// on any platform.  DON'T CHANGE IT!
        ///
        /// This is copied right out of LoopCacheLib.  It needs to work exactly the same
        /// in both places.
        ///
        /// If you implement this in another language, make sure it generates hash codes
        /// that match what you get from C#.
        ///
        /// This method is used to predict which data node we should be talking to.
        ///
        /// </remarks>
        /// <param name="guid"></param>
        /// <returns></returns>
        public static int GetConsistentHashCode(string s)
        {
            if (s == null) return 0;

            if (hashCodes.ContainsKey(s))
                return hashCodes[s];

            MD5 md5 = MD5.Create();
            byte[] hash = md5.ComputeHash(Encoding.ASCII.GetBytes(s));
            int a = BitConverter.ToInt32(hash, 0);
            int b = BitConverter.ToInt32(hash, 4);
            int c = BitConverter.ToInt32(hash, 8);
            int d = BitConverter.ToInt32(hash, 12);

            int hashCode = (a ^ b ^ c ^ d);
            hashCodes[s] = hashCode;

            return hashCode;
        }

        /// <summary>
        /// Get the data node that owns this key
        /// </summary>
        /// <remarks>
        /// Virtual nodes are on a 32 bit ring.  Find the location 
        /// of the key's hash and the node that owns it is the first node
        /// we find from that point upwards.
        /// </remarks>
        private static Node GetNodeForKey(string key)
        {
            Node returnValue = null;

            int hashCode = CacheBase.GetConsistentHashCode(key);

            var keys = CacheBase.RingNodes.Keys;

            for (int i = 0; i < keys.Count; i++)
            {
                if (keys[i] >= hashCode)
                {
                    returnValue = CacheBase.RingNodes[keys[i]];
                    break;
                }
            }

            if (returnValue == null)
                returnValue = CacheBase.RingNodes[keys[0]];

            return returnValue;
        }

        /// <summary>
        /// Look up the host and create an IP end point based on the hostname and port.
        /// </summary>
        /// <param name="hostname"></param>
        /// <param name="port"></param>
        /// <returns></returns>
        private static IPEndPoint GetIPEndPoint(string hostname, int port)
        {
            IPEndPoint returnValue = null;

            IPAddress[] ips = Dns.GetHostAddresses(hostname);

            foreach (IPAddress ip in ips)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    returnValue = new IPEndPoint(ip, port);
                }
            }

            if (returnValue == null)
                throw new Exception("Unable to resolve address");

            return returnValue;
        }

        protected static byte[] ToByteArray(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        protected static byte[] ToByteArray(object o)
        {
            if (o == null)
            {
                return null;
            }
            var bf = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                bf.Serialize(ms, o);
                return ms.ToArray();
            }
        }

        protected static T FromByteArray<T>(byte[] b)
        {
            if (b == null)
            {
                return default(T);
            }
            using (var memStream = new MemoryStream())
            {
                var binForm = new BinaryFormatter();
                memStream.Write(b, 0, b.Length);
                memStream.Seek(0, SeekOrigin.Begin);
                var obj = (T)binForm.Deserialize(memStream);
                return obj;
            }
        }

        protected static string Read(byte[] b)
        {
            return Encoding.UTF8.GetString(b);
        }

        protected static int Read(int i)
        {
            return IPAddress.NetworkToHostOrder(i);
        }

        protected static long Read(long i)
        {
            return IPAddress.NetworkToHostOrder(i);
        }

        #endregion
    }
}
