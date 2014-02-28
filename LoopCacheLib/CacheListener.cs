using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LoopCacheLib
{
	/// <summary>Listens on a port for cache requests from clients</summary>
	/// <remarks>Depending on configuration, can be a master or data node</remarks>
	public class CacheListener
	{
		// Remember: FAST FAST FAST

		/// <summary>This is the object cache</summary>
		private SortedList<string, byte[]> dataByKey;

		/// <summary>A list of keys sorted by put time</summary>
		/// <remarks>Each datetime is a bucket of keys, since more than one may be
		/// accessed at the same time.  </remarks>
		private SortedList<DateTime, List<string>> keysByTime;

		/// <summary>The last time objects were saved</summary>
		private SortedList<string, DateTime> keyPutTimes;

		/// <summary>Synchronizes access to the data in the cache</summary>
		private ReaderWriterLockSlim dataLock = new ReaderWriterLockSlim();

		/// <summary>Configuration for this listener.</summary>
		private CacheConfig config;

		/// <summary>The location of the config file</summary>
		/// <remarks>The listener can edit the file if it's the master</remarks>
		private string configFilePath;

		/// <summary>The IP and Port that we're listening on</summary>
		private IPEndPoint ipep;

		/// <summary>If set to false, stop accepting new client requests</summary>
		private bool shouldRun;

		/// <summary>The TCP Listener</summary>
		private TcpListener listener;

		/// <summary>Create an uninitialized instance of the listener</summary>
		/// <remarks>This constructor is for testing</remarks>
		public CacheListener()
		{
			this.dataByKey = new SortedList<string, byte[]>();
			this.keysByTime = new SortedList<DateTime, List<string>>();
			this.keyPutTimes = new SortedList<string, DateTime>();
		}

		/// <summary>Create a new listener based on a config file</summary>
		public CacheListener(string configFilePath) : this()
		{
			this.configFilePath = configFilePath;
			this.config = CacheConfig.Load(configFilePath);
			if (config.IsTraceEnabled) 
			{
				CacheHelper.TraceFilePath = config.TraceFilePath;
			}
			this.ipep = new IPEndPoint(IPAddress.Parse(config.ListenerIP), 
					config.ListenerPortNumber);
		}

		/// <summary>Register with the master and get the cache ring</summary>
		CacheRing RegisterWithMaster()
		{
			CacheRing ring = null;

			// TODO
			//

			return ring;
		}

		/// <summary>
		/// Start listening for client requests.
		/// </summary>
		/// <remarks>This is an async method so it returns control to the 
		/// caller as soon as it starts listening.</remarks>
		public async Task<bool> StartAsync()
		{
			Console.WriteLine("Start");

			if (this.config.IsMaster)
			{
				this.config.Ring.DetermineNodeLocations();
			}
			else
			{
				this.config.Ring = RegisterWithMaster();
			}

			this.shouldRun = true;

			this.listener = new TcpListener(this.ipep.Address, this.ipep.Port);

			listener.Start();

			Console.WriteLine("TcpListener started");
			
			try
			{
				while (this.shouldRun)
				{
					Console.WriteLine("In while loop");

					// The "await" causes us to return to the caller upon the 
					// first iteration of the while loop.  Accept gets called
					// when AcceptTcpClientAsync returns.  We don't use the 
					// task variable here, it's only there to avoid a compiler warning.
					// We could use it to force completion of Accept and check for exceptions.
					var task = Accept(await listener.AcceptTcpClientAsync());

					Console.WriteLine("After var task");
				}
			}
			catch (Exception ex)
			{
				if (shouldRun)
				{
					CacheHelper.LogError(ex.ToString());
					return false;
				}
			}
			finally
			{
				Console.WriteLine("In finally");

				listener.Stop();
			}

			Console.WriteLine("StartAsync returning");
			return true;
		}

		/// <summary>
		/// Accept a new client TCP request.
		/// </summary>
		/// <remarks>This is an asynch method so it returns control back to the caller 
		/// before it completes.</remarks>
		/// <param name="client"></param>
		/// <returns></returns>
		async Task Accept(TcpClient client)
		{
			// This forces the method to return immediately to the caller, so 
			// we can call AcceptTcpClientAsync to be ready for the next connection.
			await Task.Yield();

			try
			{
				using (client)
				{
					using (NetworkStream n = client.GetStream())
					{
						// Return to the caller while waiting for the request
						var request = await GetRequestFromNetworkStream(n);

						// Once the request is read, control returns to this method here

						// Process the request and create a response message
						CacheMessage response = ProcessRequest(request);

						// Return to the caller while waiting to write the response
						await WriteResponse(n, response.MessageType, response.Data);
					}
				}
			}
			catch (Exception ex)
			{
				CacheHelper.LogTrace(ex.ToString());
			}
		}

        private CacheMessage ProcessRequest(CacheMessage request)
		{
			try
			{
				CacheRequestTypes t = (CacheRequestTypes)request.MessageType;
				byte[] data = request.Data;

				switch (t)
				{
					case CacheRequestTypes.GetConfig: return GetConfig(data);
					case CacheRequestTypes.NodeDown: return NodeDown(data);
					case CacheRequestTypes.AddNode: return AddNode(data);
					case CacheRequestTypes.RemoveNode: return RemoveNode(data);
					case CacheRequestTypes.ChangeNode: return ChangeNode(data);
					case CacheRequestTypes.GetStats: return GetStats(data);
					case CacheRequestTypes.GetObject: return GetObject(data);
					case CacheRequestTypes.PutObject: return PutObject(data);
					case CacheRequestTypes.DeleteObject: return DeleteObject(data);
					case CacheRequestTypes.ChangeConfig: return ChangeConfig(data);

					default: 
						CacheMessage response = new CacheMessage();
						response.MessageType = (byte)CacheResponseTypes.InvalidRequestType;
						response.Data = new byte[] {};
						return response;
				}
			}
			catch (Exception ex)
			{
				// Anything that causes this exception is a bug we need to fix
				CacheHelper.LogError(ex.ToString());
				CacheMessage response = new CacheMessage();
				response.MessageType = (byte)CacheResponseTypes.InternalServerError;
				return response;
			}
		}

		/// <summary>A request for the node's current configuration</summary>
		/// <remarks>Data nodes request config from the master when they get a 
		/// request from a client for something the data node doesn't think 
		/// it owns.  If the client is wrong, the new config is sent to the 
		/// client.  If the data node is wrong, it corrects its config and then
		/// finishes responding to the client.  Responds with CacheMessageTypes.Configuration
        /// and the data is a UTF8 string, one line per node config, terminated by
        /// CRLF.  It's the same as what's in the config file for the nodes.</remarks>
		public CacheMessage GetConfig(byte[] data)
		{
			CacheMessage response = new CacheMessage();

            response.MessageType = (byte)CacheResponseTypes.Configuration;

			// TODO - Custom binary format

			return response;
		}

		/// <summary>A report from a client that a node is not responding</summary>
		public CacheMessage NodeDown(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>A request for the master node to add a data node and push the
		/// change out to all data nodes.</summary>
		public CacheMessage AddNode(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>A request for the master node to remove a data node and push the
		/// change out to all data nodes.</summary>
		public CacheMessage RemoveNode(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>A request for the master node to make a change to a node and push the
		/// change out to all data nodes</summary>
		public CacheMessage ChangeNode(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>A request for node statistics</summary>
		public CacheMessage GetStats(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>A request from a client to a data node to retrieve a cached object
		/// </summary>
		public CacheMessage GetObject(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>Updates the ring configuration based on a call to master</summary>
		private void GetConfigFromMaster()
		{

		}

		/// <summary>Serializes the config for clients and other nodes</summary>
		private byte[] SerializeConfig()
		{
			MemoryStream ms = new MemoryStream();

			using (BinaryWriter w = new BinaryWriter(ms))
			{
				// TODO
			}

			return ms.ToArray();
		}

		/// <summary>Starts the rebalancer</summary>
		private void StartRebalance()
		{
			// This is something we want to happen on a background thread
			Thread t = new Thread(Rebalance);
			t.Start();

			CacheHelper.LogInfo("Started Rebalance Thread");
		}

		/// <summary>Moves all keys that don't belong here any more to the node where
		/// they actually belong, according to current configuration</summary>
		private void Rebalance()
		{
			try
			{
				// TODO - Do this without slowing down puts and gets
			}
			catch (Exception ex)
			{
				CacheHelper.LogError(ex.ToString());
			}
		}

		/// <summary>A request from a client to a data node to store an object in the cache.
		/// </summary>
		/// <param name="messageData">The entire message.  keyLen, key, dataLen, data</param>
		public CacheMessage PutObject(byte[] messageData)
		{
			if (!this.config.IsDataNode)
			{
				var msg = "This is not a data node";
				CacheMessage response = new CacheMessage();
				response.MessageType = (byte)CacheResponseTypes.NotDataNode;
				CacheHelper.LogTrace(msg);
				response.StringData = msg;
				return response;
			}

			using (MemoryStream ms = new MemoryStream(messageData))
			{
				BinaryReader br = new BinaryReader(ms);
				byte[] key;
				if (!TryReadArray(br, out key))
				{
					var msg = "PutObject unable to read key";
					CacheHelper.LogTrace(msg);
					CacheMessage response = new CacheMessage();
					response.MessageType = (byte)CacheResponseTypes.ReadKeyError;
					return response;
				}
				byte[] objectData;
				if (!TryReadArray(br, out objectData))
				{
					var msg = "PutObject unable to read data";
					CacheHelper.LogTrace(msg);
					CacheMessage response = new CacheMessage();
					response.MessageType = (byte)CacheResponseTypes.ReadDataError;
					return response;
				}

				// Make sure key is a string
				string keyString = null;
				try
				{
					keyString = Encoding.UTF8.GetString(key);
				}
				catch
				{
					var msg = "PutObject key is not a string";
					CacheHelper.LogTrace(msg);
					CacheMessage response = new CacheMessage();
					response.MessageType = (byte)CacheResponseTypes.ReadKeyError;
					response.StringData = msg;
					return response;
				}

				// Hash the key, make sure this node owns it
				int hash = CacheHelper.GetConsistentHashCode(keyString);
				CacheNode node = this.config.Ring.GetNodeForHash(hash);
				if (IsThisNode(node))
				{
					return PutObject(keyString, objectData);
				}
				else
				{
					// A client asked this node to store an object that doesn't 
					// match up with this node's virtual node locations.  Since clients
					// and nodes cache the ring, there might have been changes since we
					// started, so let's check with master to see if the config changed.

					// Check with master for possibly new config
					GetConfigFromMaster();

					// See if the node location changed
					node = this.config.Ring.GetNodeForHash(hash);

					if (IsThisNode(node))
					{
						// This means the client had it right and this node had it wrong
						return PutObject(keyString, objectData);

						// The master is supposed to tell us about changes, but sometimes
						// we might hear it from a client first.  Rabalance regardless.
						StartRebalance();
					}
					else
					{
						// This means the client is still wrong
						CacheMessage response = new CacheMessage();
						response.MessageType = (byte)CacheResponseTypes.ReConfigure;
						response.Data = SerializeConfig();
						return response;
					}
				}
			}
		}

		private CacheMessage PutObject(string key, byte[] data)
		{
			CacheMessage response = new CacheMessage();

			try
			{
				// We have to lock everything down to add something to the cache
				this.dataLock.EnterWriteLock();

				// Add or update the object
				if (this.dataByKey.ContainsKey(key))
				{
					this.dataByKey[key] = data;
				}
				else
				{
					this.dataByKey.Add(key, data);
				}

				DateTime now = DateTime.UtcNow;

				// Update keys by time for LRU logic

				if (this.keyPutTimes.ContainsKey(key))
				{
					// Replace the put time if it was already there
					DateTime oldDateTime = this.keyPutTimes[key];
					this.keyPutTimes[key] = now;

					// We have to clear out any stale put times for this key
					if (this.keysByTime.ContainsKey(oldDateTime))
					{
						List<string> keysAtOldTime = this.keysByTime[oldDateTime];
						keysAtOldTime.Remove(key);
					}
				}
				else
				{
					this.keyPutTimes.Add(key, now);
				}

				// Add the key to the bucket of objects being saved right now.
				List<string> keysNow;
				if (this.keysByTime.ContainsKey(now))
				{
					keysNow = this.keysByTime[now];
				}
				else
				{
					keysNow = new List<string>();
					this.keysByTime.Add(now, keysNow);
				}
				if (!keysNow.Contains(key))
				{
					keysNow.Add(key);
				}

				response.MessageType = (byte)CacheResponseTypes.ObjectOk;
			}
			catch (Exception ex)
			{
				CacheHelper.LogError(ex.ToString());
				response.MessageType = (byte)CacheResponseTypes.InternalServerError;
			}
			finally
			{
				this.dataLock.ExitWriteLock();
			}

			return response;
		}

		bool IsThisNode(CacheNode node)
		{
			if (node.HostName.Equals(this.config.ListenerHostName) && 
				node.PortNumber.Equals(this.config.ListenerPortNumber))
			{
				return true;
			}
			return false;
		}

		/// <summary>Try to read {Length:int}{Data:byte[]} from the stream</summary>
		bool TryReadArray(BinaryReader r, out byte[] data)
		{
			try
			{
				int len = IPAddress.NetworkToHostOrder(r.ReadInt32());
				data = new byte[len];
				if (r.Read(data, 0, len) != len) return true;
				else return false;
			}
			catch (Exception ex)
			{
				CacheHelper.LogTrace(ex.ToString());
				data = new byte[0];
				return false;
			}
		}

		/// <summary>A request from a client to a data node to remove an object from the cache
		/// </summary>
		public CacheMessage DeleteObject(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>A request from the master to a data node to change its configuration.
		/// </summary>
		public CacheMessage ChangeConfig(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>A request from a data node on startup to the master, to let the 
		/// master know the data node is ready, and to get config from the master.
		/// </summary>
		public CacheMessage Register(byte[] data)
		{
			CacheMessage response = new CacheMessage();

			// TODO

			return response;
		}

		/// <summary>
		/// Write a response to the network stream.
		/// </summary>
		/// <param name="n"></param>
		/// <param name="responseType"></param>
		/// <param name="responseData"></param>
		/// <returns></returns>
		async Task WriteResponse(NetworkStream n, byte responseType, byte[] responseData)
		{
			await Task.Yield();

			BinaryWriter w = new BinaryWriter(n);
			w.Write(responseType);
			w.Write(IPAddress.HostToNetworkOrder(responseData.Length));
			w.Write(responseData);
			w.Flush();
		}

		/// <summary>
		/// Read the complete client request from the stream and deserialize it.
		/// </summary>
		/// <remarks>This is an async method so it might return control to the caller
		/// while it waits for bytes to come across the network.</remarks>
		/// <param name="n"></param>
		/// <returns></returns>
		async Task<CacheMessage> GetRequestFromNetworkStream(NetworkStream n)
		{
			// Read the 1st byte, which is the message type
			byte[] messageTypeBuf = new byte[1];
			byte messageType = 0;
			if (n.Read(messageTypeBuf, 0, 1) > 0)
			{
				messageType = messageTypeBuf[0];
			}
			else
			{
				throw new Exception("stream was empty trying to get messageType");
			}

			// Not sure if a buffer is necessary for reading 4 bytes, 
			// but maybe it's possible to ask for 4 bytes and get less.
			byte[] lenbuf = new byte[4];
			int lenBytesRead = 0;
			int lenChunkSize = 1;
			while (lenBytesRead < lenbuf.Length && lenChunkSize > 0)
			{
				lenBytesRead += lenChunkSize =
					n.Read(lenbuf, lenBytesRead, lenbuf.Length - lenBytesRead);
			}
			if (BitConverter.IsLittleEndian)
				Array.Reverse(lenbuf);
			int dataLength = BitConverter.ToInt32(lenbuf, 0);

			byte[] data = new byte[dataLength];
			int bytesRead = 0;
			int chunkSize = 1;

			// TODO - Buffer
			while (bytesRead < data.Length && chunkSize > 0)
			{
				bytesRead += chunkSize =
					await n.ReadAsync(data, bytesRead, data.Length - bytesRead);
			}

			CacheMessage m = new CacheMessage();
			m.MessageType = messageType;
			m.Data = data;

			return m;
		}

		public void Stop()
		{
			this.shouldRun = false;
			this.listener.Stop();
		}
	}


}

