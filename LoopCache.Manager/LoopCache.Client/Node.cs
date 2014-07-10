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
    /// The client's representation of a data node.
    /// </summary>
    public class Node
    {
        /// <summary>Represents the status of a node</summary>
        public enum StatusType
        {
            /// <summary>Node is down temporarily but still owns its objects</summary>
            /// <remarks>This is usually during startup, before a node has registered</remarks>
            Down = 1,

            /// <summary>The node is fully operational</summary>
            Up = 2,

            /// <summary>Clients are reporting that this node is not available.</summary>
            /// <remarks>Also set when the master tries to send a data node something
            /// and it doesn't get a response.  Nodes will never be taken down or 
            /// removed automatically, so this status is how the code brings a node
            /// to an administrator's attention.</remarks>
            Questionable = 3,

            /// <summary>
            /// The node is being shut down gracefully and is migrating its objects.
            /// </summary>
            /// <remarks>
            /// It no longer owns its objects and is not part of the cache ring.
            /// </remarks>
            Migrating = 4
        }

        public StatusType Status { get; set; }

        /// <summary>
        /// Max number of bytes the node can handle
        /// </summary>
        public long MaxNumBytes { get; set; }

        public Node(string hostName, int port)
        {
            this.HostName = hostName;
            this.Port = port;
            this.Name = string.Format("{0}:{1}", hostName, port).ToUpper();
        }

        /// <summary>
        /// Create an instance based on all its properties.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="maxNumBytes"></param>
        /// <param name="ipep"></param>
        public Node(string hostName, int port, long maxNumBytes, StatusType status)
        {
            this.HostName = hostName;
            this.Port = port;
            this.MaxNumBytes = maxNumBytes;
            this.Status = status;
            this.Name = string.Format("{0}:{1}", hostName, port).ToUpper();
        }

        public string HostName { get; private set; }
        public int Port { get; private set; }
        public string Name { get; private set; }
        //
        //  Stats
        //
        public int NumObjects { get; set; }
        public long TotalDataBytes { get; set; }
        public long LatestRAMBytes { get; set; }
        public decimal RAMMultiplier { get; set; }

        public void GetStats()
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
            Response response = Common.SendMessage(this.HostName, this.Port, request);

            if (response == null || response.Data.Length == 0)
            {
                this.Status = StatusType.Questionable;
                return;
            }

            if (response.Data.Length > 0)
            {
                using (MemoryStream ms = new MemoryStream(response.Data))
                {
                    using (BinaryReader reader = new BinaryReader(ms))
                    {
                        this.NumObjects         = Common.Read(reader.ReadInt32());
                        this.TotalDataBytes     = Common.Read(reader.ReadInt64());
                        this.LatestRAMBytes     = Common.Read(reader.ReadInt64());

                        int readLen             = Common.Read(reader.ReadInt32());
                        byte[] tempData = new byte[readLen];

                        if (reader.Read(tempData, 0, readLen) != readLen)
                            throw new Exception("Invalid RAM Multipler Data");

                        string temp             = Common.Read(tempData);
                        this.RAMMultiplier = decimal.Parse(temp);

                        this.MaxNumBytes        = Common.Read(reader.ReadInt64());
                        this.Status = (Node.StatusType)reader.ReadByte();
                    }
                }
            }
        }
    }
}
