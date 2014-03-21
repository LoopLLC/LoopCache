using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
// DO NOT import LoopCacheLib!

namespace LoopCacheConsole
{
    /// <summary>This is the client used by the console application to 
    /// test and administer the cache.</summary>
    /// <remarks>This class can be used as an example of how to write a client.
    /// Remember that client code will *not* have access to LoopCacheLib, since
    /// that library is only used by the server process.</remarks>
    public class CacheClient
    {
        /// <summary>
        /// The master node
        /// </summary>
        private IPEndPoint masterNode;

        /// <summary>
        /// A map of the virtual node locations
        /// </summary>
        private SortedList<int, Node> sortedLocations;

        /// <summary>
        /// A list of "host:port" => Node
        /// </summary>
        private SortedList<string, Node> dataNodes;

        /// <summary>
        /// Construct a new instance based on the master node's hostname:port
        /// </summary>
        /// <param name="hostNamePort">The master node</param>
        public CacheClient(string hostNamePort)
        {
            this.sortedLocations = new SortedList<int, Node>();
            this.dataNodes = new SortedList<string, Node>();

            this.masterNode = GetIPEndPoint(hostNamePort);
        }

        /// <summary>
        /// Look up the host and create an IP end point based on the hostname and port.
        /// </summary>
        /// <param name="hostname"></param>
        /// <param name="port"></param>
        /// <returns></returns>
        private IPEndPoint GetIPEndPoint(string hostname, int port)
        {
            IPAddress[] ips = Dns.GetHostAddresses(hostname);

            foreach (IPAddress ip in ips)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return new IPEndPoint(IPAddress.Parse(ip.ToString()), port);
                }
            }

            throw new Exception("Unable to resolve address");
        }

        /// <summary>
        /// Parse the host name and port and lookup the ip end point.
        /// </summary>
        /// <param name="hostNamePort"></param>
        /// <returns></returns>
        private IPEndPoint GetIPEndPoint(string hostNamePort)
        {
            string[] tokens = hostNamePort.Split(':');
            if (tokens.Length != 2)
            {
                throw new Exception("Expected hostname:port");
            }
            string hostname = tokens[0];
            string portstr = tokens[1];
            int port;
            if (!int.TryParse(portstr, out port))
            {
                throw new Exception("Invalid port");
            }

            return GetIPEndPoint(hostname, port);

        }

        /// <summary>
        /// Get the data node that owns this key
        /// </summary>
        /// <remarks>
        /// Virtual nodes are on a 32 bit ring.  Find the location 
        /// of the key's hash and the node that owns it is the first node
        /// we find from that point upwards.
        /// </remarks>
        private IPEndPoint GetNodeForKey(string key)
        {
            int hashCode = GetConsistentHashCode(key);
            IPEndPoint firstNode = null;
            foreach (var kvp in this.sortedLocations)
            {
                if (firstNode == null) firstNode = kvp.Value.EndPoint;
                if (kvp.Key >= hashCode) return kvp.Value.EndPoint;
            }

            // We wrapped around Int.MAX
            return firstNode;
        }

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
            MD5 md5 = MD5.Create();
            byte[] hash = md5.ComputeHash(Encoding.ASCII.GetBytes(s));
            int a = BitConverter.ToInt32(hash, 0);
            int b = BitConverter.ToInt32(hash, 4);
            int c = BitConverter.ToInt32(hash, 8);
            int d = BitConverter.ToInt32(hash, 12);
            return (a ^ b ^ c ^ d);
        }

        /// <summary>
        /// Send a message to a server.
        /// </summary>
        /// <param name="server"></param>
        /// <param name="messageType"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        private Tuple<byte, byte[]> SendMessage(IPEndPoint server, 
                byte messageType, byte[] data)
        {
            // Create a new client to talk to the server
            using (TcpClient client = new TcpClient())
            {
                // Connect to the server
                client.Connect(server);

                using (NetworkStream stream = client.GetStream())
                {
                    // Write the request
                    BinaryWriter w = new BinaryWriter(stream);
                    w.Write(messageType);
                    w.Write(IPAddress.HostToNetworkOrder(data.Length));
                    w.Write(data);
                    w.Flush();

                    // Read the response
                    BinaryReader r = new BinaryReader(stream);
                    byte responseType = r.ReadByte();
                    int responseLength = IPAddress.NetworkToHostOrder(r.ReadInt32());
                    if (responseLength > MaxLength)
                    {
                        throw new Exception(
                            "SendMessage got a response that exceeded max length");
                    }
                    byte[] responseData = r.ReadBytes(responseLength);

                    return new Tuple<byte, byte[]>(responseType, responseData);
                }
            }
        }

        private const byte Request_GetConfig         = 1;
        private const byte Request_NodeDown          = 2;
        private const byte Request_AddNode           = 3;
        private const byte Request_RemoveNode        = 4;
        private const byte Request_ChangeNode        = 5;
        private const byte Request_GetStats          = 6;
        private const byte Request_GetObject         = 7;
        private const byte Request_PutObject         = 8;
        private const byte Request_DeleteObject      = 9;
        private const byte Request_ChangeConfig      = 10;
        private const byte Request_Register          = 11;
        private const byte Request_Ping              = 12;

        private const byte Response_InvalidRequestType       = 1;
        private const byte Response_NotMasterNode            = 2; 
        private const byte Response_NotDataNode              = 3; 
        private const byte Response_ObjectOk                 = 4; 
        private const byte Response_ObjectMissing            = 5;
        private const byte Response_ReConfigure              = 6;
        private const byte Response_Configuration            = 7;
        private const byte Response_InternalServerError      = 8;
        private const byte Response_ReadKeyError             = 9;
        private const byte Response_ReadDataError            = 10;
        private const byte Response_UnknownNode              = 11;
        private const byte Response_EndPointMismatch         = 12;
        private const byte Response_NodeExists               = 13;
        private const byte Response_Accepted                 = 14;
        private const byte Response_DataNodeNotReady         = 15;

        private const int MaxLength = 1024 * 1024 * 1024; // 1Gb

        /// <summary>
        /// Test the server.
        /// </summary>
        /// <remarks>
        /// This requires a master node to be running based on SampleDevMaster2.txt, 
        /// a data node using SampleDevDataNode.txt, and another node using 
        /// SampleDevDataNode2.txt.  So you'll want 4 console windows open total.
        /// </remarks>
        /// <returns></returns>
        public bool Test()
        {
            if (GetConfig() &&
                NodeDown() && 
                AddNode("localhost:12347", 2048) &&
                Pause(1) && 
                ChangeNode("localhost:12347", 4096) &&
                Pause(1) &&
                RemoveNode("localhost:12347") && 
                Pause(1) &&
                GetStats() && 
                PutObject("abc", "Hello, World!") && 
                GetObject("abc", "Hello, World!") &&
                DeleteObject("abc") && 
                ChangeConfig() && 
                TestThreads())
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Used by Test method to wait between tests.
        /// </summary>
        /// <param name="numSeconds"></param>
        /// <returns></returns>
        private bool Pause(int numSeconds)
        {
            Thread.Sleep(1000 * numSeconds);
            return true;
        }

        /// <summary>
        /// Convert a byte array to a string using UTF8 encoding.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private string BytesToString(byte[] data)
        {
            MemoryStream ms = new MemoryStream(data);
            using (StreamReader sr = new StreamReader(ms))
            {
                // Should be the same as Encoding.UTF8.GetString(byte[])
                return sr.ReadToEnd();
            }
        }

        /// <summary>
        /// Reload the ring configuratio from master.
        /// </summary>
        /// <returns></returns>
        public bool GetConfig() 
        {
            byte[] data = new byte[0];
            var response = SendMessage(this.masterNode, Request_GetConfig, data);
            if (response == null)
            {
                Console.WriteLine("GetConfig got a null response");
                return false;
            }
            if (response.Item1 != Response_Configuration)
            {
                Console.WriteLine("Got {0} instead of {1} for GetConfig", 
                        response.Item1, Response_Configuration);
                return false;
            }

            // GetConfig binary response format:

            // NumNodes            int
            // [
            //     HostLen         int
            //     Host            byte[] UTF8 string
            //     Port            int
            //     MaxNumBytes     long
            //     NumLocations    int
            //     [Locations]     ints
            // ]

            return ReadConfigBytes(response.Item2);
        }

        private bool ReadConfigBytes(byte[] data)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(ms))
                {
                    int numNodes = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                    for (int n = 0; n < numNodes; n++)
                    {
                        int hostLen = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                        byte[] hostData = new byte[hostLen];
                        if (reader.Read(hostData, 0, hostLen) != hostLen)
                        {
                            Console.WriteLine("Invalid GetConfig hostData");
                            return false;
                        }
                        string hostname = Encoding.UTF8.GetString(hostData);
                        int port = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                        IPEndPoint ipep = GetIPEndPoint(hostname, port);
                        long maxNumBytes = IPAddress.NetworkToHostOrder(reader.ReadInt64());

                        Node node = new Node(hostname, port, maxNumBytes, ipep);
                        this.dataNodes.Add(string.Format("{0}:{1}", 
                            hostname, port), node);

                        int numLocations = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                        for (int i = 0; i < numLocations; i++)
                        {
                            int location = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                            if (this.sortedLocations.ContainsKey(location))
                            {
                                Console.WriteLine("Already saw location {0}", location);
                                return false;
                            }
                            this.sortedLocations.Add(location, node);
                        }
                    }
                }
            }

            return true;
        }

        public bool NodeDown()
        {
            // TODO
            return true;
        }

        /// <summary>
        /// Add a node to the cluster.
        /// </summary>
        /// <remarks>The node needs to be up and running already.</remarks>
        /// <param name="hostPort">hostname:port for the new node</param>
        /// <param name="maxNumBytes">Max number of bytes the new node can handle</param>
        /// <returns>true if the message was delivered to master successfully, but
        /// the master does some work in the background after returning, so 
        /// it's possible for something to fail afterwards.</returns>
        public bool AddNode(string hostPort, long maxNumBytes)
        {
            string[] tokens = hostPort.Split(':');
            if (tokens.Length != 2)
            {
                throw new Exception("Expected hostname:port");
            }
            string hostname = tokens[0];
            string portstr = tokens[1];
            int port;
            if (!int.TryParse(portstr, out port))
            {
                throw new Exception("Invalid port");
            }

            //     Request Layout:

            //          HostLen         int
            //          Host            byte[] UTF8 string
            //          Port            int
            //          MaxNumBytes     long

            byte[] message = null;
            using (MemoryStream ms = new MemoryStream())
            {
                BinaryWriter w = new BinaryWriter(ms);

                byte[] hostBytes = Encoding.UTF8.GetBytes(hostname);
                w.Write(IPAddress.HostToNetworkOrder(hostBytes.Length));
                w.Write(hostBytes);
                w.Write(IPAddress.HostToNetworkOrder(port));
                w.Write(IPAddress.HostToNetworkOrder(maxNumBytes));

                w.Flush();
                ms.Flush();
                message = ms.ToArray();
            }

            var response = SendMessage(masterNode, Request_AddNode, message);
            if (response.Item1 != Response_Accepted)
            {
                Console.WriteLine("Got {0} instead of {1} for AddNode",
                    response.Item1, Response_Accepted);
                return false; 
            }

            return true;
        }

        /// <summary>
        /// Remove a node from the cluster.
        /// </summary>
        /// <param name="hostPort"></param>
        /// <returns></returns>
        public bool RemoveNode(string hostPort)
        {
            string[] tokens = hostPort.Split(':');
            if (tokens.Length != 2)
            {
                throw new Exception("Expected hostname:port");
            }
            string hostname = tokens[0];
            string portstr = tokens[1];
            int port;
            if (!int.TryParse(portstr, out port))
            {
                throw new Exception("Invalid port");
            }

            //     Request Layout:

            //          HostLen         int
            //          Host            byte[] UTF8 string
            //          Port            int

            byte[] message = null;
            using (MemoryStream ms = new MemoryStream())
            {
                BinaryWriter w = new BinaryWriter(ms);

                byte[] hostBytes = Encoding.UTF8.GetBytes(hostname);
                w.Write(IPAddress.HostToNetworkOrder(hostBytes.Length));
                w.Write(hostBytes);
                w.Write(IPAddress.HostToNetworkOrder(port));

                w.Flush();
                ms.Flush();
                message = ms.ToArray();
            }

            var response = SendMessage(masterNode, Request_RemoveNode, message);
            if (response.Item1 != Response_Accepted)
            {
                Console.WriteLine("Got {0} instead of {1} for RemoveNode",
                    response.Item1, Response_Accepted);
                return false;
            }

            return true;
        }

        public bool ChangeNode(string hostPort, long maxNumBytes)
        {
            // TODO
            return true;
        }

        public bool GetStats()
        {
            // TODO
            return true;
        }

        public bool GetObject(string key, string expectedValue)
        {
            // Make sure the data we get back matches what's passed in
            IPEndPoint node = GetNodeForKey(key);

            var response = SendMessage(node, Request_GetObject, Encoding.UTF8.GetBytes(key));
            if (response == null)
            {
                Console.WriteLine("GetObject got a null response");
                return false;
            }
            byte expected = Response_ObjectOk;
            if (response.Item1 != expected)
            {
                Console.WriteLine("Got {0} instead of {1} for GetObject", 
                        response.Item1, expected);
                return false;
            }

            string objectString = Encoding.UTF8.GetString(response.Item2);
            if (objectString.Equals(expectedValue)) return true;
            else
            {
                Console.WriteLine("Got [{0}] instead of [{1}]", 
                    objectString, expectedValue);
                return false;
            }
            
        }

        public bool PutObject(string keyString, string dataString)
        {
            // PutObject binary data format:
            // 
            // KeyLength        int 
            // Key              byte[] UTF8 String 
            // ObjectLength     int 
            // Object           byte[]

            byte[] message = null;
            using (MemoryStream ms = new MemoryStream())
            {
                BinaryWriter w = new BinaryWriter(ms);
                byte[] key = Encoding.UTF8.GetBytes(keyString);
                w.Write(IPAddress.HostToNetworkOrder(key.Length));
                w.Write(key);
                byte[] data = Encoding.UTF8.GetBytes(dataString);
                w.Write(IPAddress.HostToNetworkOrder(data.Length));
                w.Write(data);
                w.Flush();
                ms.Flush();
                message = ms.ToArray();
            }

            IPEndPoint node = GetNodeForKey(keyString);
            var response = SendMessage(node, Request_PutObject, message);
            if (response == null)
            {
                Console.WriteLine("PutObject got a null response");
                return false;
            }
            byte expected = Response_ObjectOk;
            if (response.Item1 != expected)
            {
                Console.WriteLine("Got response type {0} instead of {1} for PutObject", 
                        response.Item1, expected);
                return false;
            }

            return true;
        }

        public bool DeleteObject(string keyString)
        {
            IPEndPoint node = GetNodeForKey(keyString);

            var response = SendMessage(node, Request_DeleteObject, 
                Encoding.UTF8.GetBytes(keyString));

            if (response == null)
            {
                Console.WriteLine("DeleteObject got a null response");
                return false;
            }

            switch (response.Item1)
            {
                case Response_ObjectOk:
                    return true;
                case Response_ReConfigure:
                    return ReadConfigBytes(response.Item2);
                default:
                    Console.WriteLine("Got unexpected response type {0} for DeleteObject",
                        response.Item1, Response_ObjectOk);
                    return false;
            }
        }

        public bool ChangeConfig()
        {
            return true;
        }

        public bool TestThreads()
        {
            // TODO - Spawn a bunch of threads and stress-test the server
            return true;
        }

        /// <summary>
        /// Gets the latest ring configuration from the master and prints
        /// out data nodes to the console.
        /// </summary>
        /// <returns></returns>
        public void PrintList()
        {
            GetConfig();

            foreach (var kvp in this.dataNodes)
            {
                Node node = kvp.Value;
                Console.WriteLine("{0}:{1}\t{2}", node.Host, node.Port, node.MaxNumBytes);
            }
        }

        /// <summary>
        /// The client's representation of a data node.
        /// </summary>
        public class Node
        {
            /// <summary>
            /// Hostname of the node
            /// </summary>
            public string Host { get; set; }

            /// <summary>
            /// Port number the node is listening on
            /// </summary>
            public int Port { get; set; }

            /// <summary>
            /// Max number of bytes the node can handle
            /// </summary>
            public long MaxNumBytes { get; set; }

            /// <summary>
            /// The end point the node is listening on.
            /// </summary>
            public IPEndPoint EndPoint { get; set; }

            /// <summary>
            /// Create a new default instance.
            /// </summary>
            public Node(){ }

            /// <summary>
            /// Create an instance based on all its properties.
            /// </summary>
            /// <param name="host"></param>
            /// <param name="port"></param>
            /// <param name="maxNumBytes"></param>
            /// <param name="ipep"></param>
            public Node(string host, int port, long maxNumBytes, IPEndPoint ipep)
            {
                this.Host = host;
                this.Port = port;
                this.MaxNumBytes = maxNumBytes;
                this.EndPoint = ipep;
            }
        }
    }
}
