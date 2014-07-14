using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using LoopCache.Client;

namespace LoopCache.Admin
{
    /// <summary>
    /// LoopCache administration.
    /// </summary>
    public class CacheAdmin : Cache
    {
        /// <summary>
        /// Construct a new instance based on the master node's hostname:port
        /// </summary>
        /// <param name="masterHostName">The master HostName</param>
        /// <param name="masterPort">The master Port</param>
        public CacheAdmin(string masterHostName, int masterPort) : base(masterHostName, masterPort)
        {
            //BaseClient.RingNodes = new SortedList<int, Node>();
            //BaseClient.Nodes = new SortedList<string, Node>();

            //if (BaseClient.RingNodes.Count == 0)
            //    base.GetConfig();
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

                byte[] hostBytes = CacheBase.ToByteArray(hostname);
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
            var response = base.SendRequest(base.MasterHostName, base.MasterPort, request);

            if (response == null)
                return false;

            return (response.Type == Response.Types.Accepted);
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
        public bool ChangeNode(string hostname, int port, long maxNumBytes)
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

                byte[] hostBytes = CacheBase.ToByteArray(hostname);
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

            Request request = new Request(Request.Types.ChangeNode, data);
            var response = base.SendRequest(base.MasterHostName, base.MasterPort, request);

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

                byte[] hostBytes = CacheBase.ToByteArray(hostname);
                w.Write(IPAddress.HostToNetworkOrder(hostBytes.Length));
                w.Write(hostBytes);
                w.Write(IPAddress.HostToNetworkOrder(port));

                w.Flush();
                ms.Flush();
                data = ms.ToArray();
            }

            Request request = new Request(Request.Types.RemoveNode, data);
            var response = base.SendRequest(base.MasterHostName, base.MasterPort, request);

            if (response == null)
                return false;

            return (response.Type != Response.Types.Accepted);
        }

        public List<Node> GetRingStats()
        {
            List<Node> returnValue = new List<Node>();

            foreach (var node in CacheBase.Nodes)
                returnValue.Add(new Node(node.Value.HostName, node.Value.Port));

            return returnValue;
        }

        private Node GetNodeStats(string hostname, int port)
        {
            // NumObjects       int
            // TotalDataBytes   long
            // LatestRAMBytes   long
            // RAMMultiplierLen int
            // RAMMultiplier    byte[] UTF8 string e.g. "1.3"
            // MaxNumBytes      long
            // Status           byte
            //      1=Down
            //      2=Up
            //      3=Questionable
            //      4=Migrating
            Request request = new Request(Request.Types.GetStats);
            Response response = base.SendRequest(hostname, port, request);

            Node returnValue = new Node(hostname, port);

            if (response == null || response.Data.Length == 0)
            {
                returnValue.Status = Node.StatusType.Questionable;
                return returnValue;
            }

            if (response.Data.Length > 0)
            {
                using (MemoryStream ms = new MemoryStream(response.Data))
                {
                    using (BinaryReader reader = new BinaryReader(ms))
                    {
                        returnValue.NumObjects = CacheBase.Read(reader.ReadInt32());
                        returnValue.TotalDataBytes = CacheBase.Read(reader.ReadInt64());
                        returnValue.LatestRAMBytes = CacheBase.Read(reader.ReadInt64());

                        int readLen = CacheBase.Read(reader.ReadInt32());
                        byte[] tempData = new byte[readLen];

                        if (reader.Read(tempData, 0, readLen) != readLen)
                            throw new Exception("Invalid RAM Multipler Data");

                        string temp = CacheBase.Read(tempData);
                        returnValue.RAMMultiplier = decimal.Parse(temp);

                        returnValue.MaxNumBytes = CacheBase.Read(reader.ReadInt64());
                        returnValue.Status = (Node.StatusType)reader.ReadByte();
                    }
                }
            }

            return returnValue;
        }

        public bool Clear()
        {
            Request request = new Request(Request.Types.Clear);
            var response = base.SendRequest(base.MasterHostName, base.MasterPort, request);

            if (response == null)
                return false;

            return (response.Type == Response.Types.Accepted);
        }
    }
}
