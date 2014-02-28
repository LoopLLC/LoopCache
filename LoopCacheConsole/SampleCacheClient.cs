using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
// DO NOT import LoopCacheLib!

namespace LoopCacheConsole
{
	/// <summary>A sample of how to write a client of LoopCache.</summary>
	/// <remarks>This class is used to test the server.  Keep in mind 
	/// that clients will not have access to LoopCacheLib, since it is 
	/// only for use by the server itself.</remarks>
	public class SampleCacheClient
	{
		/// <summary>The master node</summary>
		private IPEndPoint masterNode;

		/// <summary>A map of the virtual node locations</summary>
		private SortedList<int, IPEndPoint> sortedLocations;

		public SampleCacheClient(string hostNamePort)
		{
			this.sortedLocations = new SortedList<int, IPEndPoint>();

			this.masterNode = GetIPEndPoint(hostNamePort);
		}

		IPEndPoint GetIPEndPoint(string hostname, int port)
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

		IPEndPoint GetIPEndPoint(string hostNamePort)
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

		/// <summary>Get the data node that owns this key</summary>
		/// <remarks>Virtual nodes are on a 32 bit ring.  Find the location 
		/// of the key's hash and the node that owns it is the first node
		/// we find from that point upwards.
		IPEndPoint GetNodeForKey(string key)
		{
			int hashCode = GetConsistentHashCode(key);
			IPEndPoint firstNode = null;
			foreach (var kvp in this.sortedLocations)
			{
				if (firstNode == null) firstNode = kvp.Value;
				if (kvp.Key >= hashCode) return kvp.Value;
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

		private Tuple<byte, byte[]> SendMessage(byte messageType, byte[] data)
		{
            // Create a new client to talk to the server
			using (TcpClient client = new TcpClient())
			{
				// Connect to the server
				client.Connect(masterNode);

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
					byte[] responseData = r.ReadBytes(responseLength);

					return new Tuple<byte, byte[]>(responseType, responseData);
				}
			}
		}

		// You'd probably want to just steal the CacheRequestTypes enum from LoopCacheLib
		// instead of using constants, but constants work just fine.

		public const byte Request_GetConfig			= 1;
		public const byte Request_NodeDown		 	= 2;
		public const byte Request_AddNode	  		= 3;
		public const byte Request_RemoveNode	   	= 4;
		public const byte Request_ChangeNode	   	= 5;
		public const byte Request_GetStats		 	= 6;
		public const byte Request_GetObject			= 7;
		public const byte Request_PutObject			= 8;
		public const byte Request_DeleteObject	 	= 9;
		public const byte Request_ChangeConfig	 	= 10;

		public const byte Response_InvalidRequestType 	= 1;
		public const byte Response_NotMasterNode 		= 2; 
		public const byte Response_NotDataNode 			= 3; 
		public const byte Response_ObjectOk 			= 4; 
		public const byte Response_ObjectMissing 		= 5;
		public const byte Response_ReConfigure 			= 6;
        public const byte Response_Configuration 		= 7;

		public bool Test()
		{
			string key = "abc";
			string data = "Hello, World!";

			if (GetConfig() &&
				NodeDown() && 
				AddNode() && 
				RemoveNode() && 
				ChangeNode() && 
				GetStats() && 
				PutObject(key, data) && 
				GetObject(key, data) &&
				DeleteObject() && 
				ChangeConfig() && 
				TestThreads())
			{
				return true;
			}

			return false;
		}

		string BytesToString(byte[] data)
		{
			MemoryStream ms = new MemoryStream(data);
			using (StreamReader sr = new StreamReader(ms))
            {
				// Should be the same as Encoding.UTF8.GetString(byte[])
                return sr.ReadToEnd();
            }
		}

		bool GetConfig() 
		{
			byte[] data = new byte[0];
			var response = SendMessage(Request_GetConfig, data);
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

			// Config binary format:

			// HostLen			int
			// Host				byte[] UTF8 string
			// Port				int
			// MaxNumBytes		int
			// NumLocations		int
			// [Locations]		ints

			using (MemoryStream ms = new MemoryStream(response.Item2))
			{
				using (BinaryReader reader = new BinaryReader(ms))
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
					int maxNumBytes = IPAddress.NetworkToHostOrder(reader.ReadInt32());
					int numLocations = IPAddress.NetworkToHostOrder(reader.ReadInt32());
					for (int i = 0; i < numLocations; i++)
					{
						int location = IPAddress.NetworkToHostOrder(reader.ReadInt32());
						if (this.sortedLocations.ContainsKey(location))
						{
							Console.WriteLine("Already saw location {0}", location);
							return false;
						}
						this.sortedLocations.Add(location, ipep);
					}
				}
			}

			return true;
		}

		bool NodeDown()
		{
			return true;
		}

		bool AddNode()
		{
			return true;
		}

		bool RemoveNode()
		{
			return true;
		}

		bool ChangeNode()
		{
			return true;
		}

		bool GetStats()
		{
			return true;
		}

		bool GetObject(string key, string data)
		{
			return true;

			// Make sure the data we get back matches what's passed in

			/*
			var response = SendMessage(Request_GetObject, data);
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
			*/
		}

		bool PutObject(string testKey, string testData)
		{
			// PutObject blob layout is 
			// KeyLength:int 
			// Key:byte[] 
			// ObjectLength:int 
			// Object:byte[]

			byte[] message = null;
			using (MemoryStream ms = new MemoryStream())
			{
				BinaryWriter w = new BinaryWriter(ms);
				byte[] key = Encoding.UTF8.GetBytes(testKey);
				w.Write(IPAddress.HostToNetworkOrder(key.Length));
				w.Write(key);
				byte[] data = Encoding.UTF8.GetBytes(testData);
				w.Write(IPAddress.HostToNetworkOrder(data.Length));
				w.Write(data);
				w.Flush();
				ms.Flush();
				message = ms.ToArray();
			}

			var response = SendMessage(Request_PutObject, message);
			if (response == null)
			{
				Console.WriteLine("PutObject got a null response");
				return false;
			}
			byte expected = Response_ObjectOk;
			if (response.Item1 != expected)
			{
				Console.WriteLine("Got {0} instead of {1} for PutObject", 
						response.Item1, expected);
				return false;
			}

			return true;
		}

		bool DeleteObject()
		{
			return true;
		}

		bool ChangeConfig()
		{
			return true;
		}

		bool TestThreads()
		{
			// TODO - Spawn a bunch of threads and stress-test the server
			return true;
		}

	}
}
