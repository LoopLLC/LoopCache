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
            if (this.config.IsTraceEnabled) 
            {
                CacheHelper.TraceFilePath = config.TraceFilePath;
            }
            this.ipep = new IPEndPoint(IPAddress.Parse(this.config.ListenerIP), 
                    this.config.ListenerPortNumber);

            if (this.config.IsMaster)
            {
                // Lookup the master IP to make sure it matches the listener

                var masterIPEndPoint = 
                    CacheHelper.GetIPEndPoint(
                        config.MasterHostName, 
                        config.MasterPortNumber);
                
                if (!(this.ipep.Equals(masterIPEndPoint)))
                {
                    Console.WriteLine("Master: {0}, Listener: {1}", 
                        masterIPEndPoint, this.ipep);
                    throw new Exception(
                        "Listener misconfigured as master");
                }
            }
        }

        /// <summary>
        /// Start listening for client requests.
        /// </summary>
        /// <remarks>This is an async method so it returns control to the 
        /// caller as soon as it starts listening.</remarks>
        public async Task<bool> StartAsync()
        {
            Console.WriteLine("Start");

            this.shouldRun = true;

            try
            {
                Initialize();
            }
            catch (Exception ex)
            {
                CacheHelper.LogError(ex.ToString());
                return false;
            }

            this.listener = new TcpListener(this.ipep.Address, this.ipep.Port);

            listener.Start();

            try
            {
                while (this.shouldRun)
                {
                    // The "await" causes us to return to the caller upon the
                    // first iteration of the while loop.  Accept gets called
                    // when AcceptTcpClientAsync returns.  We don't use the
                    // task variable here, it's only there to avoid a compiler
                    // warning.  We could use it to force completion of Accept
                    // and check for exceptions.
                    var task = Accept(await listener
                            .AcceptTcpClientAsync());
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
                listener.Stop();
            }

            return true;
        }

        private void Initialize()
        {
            if (this.config.IsMaster)
                InitializeMaster();
            else
                InitializeDataNode();
        }

        private void InitializeMaster()
        {
            // Don't bother with a DNS lookup for the master endpoint
            this.config.MasterIPEndPoint = this.ipep;
        }

        private void InitializeDataNode()
        {
            // Lookup the master IP
            this.config.MasterIPEndPoint = CacheHelper.GetIPEndPoint(
                    config.MasterHostName, 
                    config.MasterPortNumber);

            this.config.Ring = null;

            int numRegisterTries = 0;
            do
            {
                // Get the ring configuration from the master
                this.config.Ring = RegisterWithMaster();

				// If we can't connect to master, pause for a while and retry.
				// Retry as many times as it takes, since we can't go any
				// further without registering and storing the cluster
				// configuration.
                if (this.config.Ring == null)
                {
                    numRegisterTries++;

                    CacheHelper.LogInfo(string.Format(
                        "Unable to register  after {0} tries", 
                        numRegisterTries));

                    // Sleep for numRegisterTries seconds
                    this.StoppablePause(numRegisterTries);
                }
            } while (this.config.Ring == null && this.shouldRun);
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
            // we can call AcceptTcpClientAsync to be ready for the next
            // connection.
            await Task.Yield();

            try
            {
                using (client)
                {
                    using (NetworkStream n = client.GetStream())
                    {
                        // Return to the caller while waiting for the request.
                        // Once the request is read, control returns to this
                        // method right after this line.
                        var request = await GetRequestFromNetworkStream(n);

                        // Set the end point in case we need to validate it
                        // later.
                        request.ClientEndPoint = (IPEndPoint)client.Client .RemoteEndPoint;

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

        /// <summary>
        /// Process a request message and generate a response message
        /// </summary>
        private CacheMessage ProcessRequest(CacheMessage request)
        {
            try
            {
                CacheRequestTypes t = (CacheRequestTypes)request.MessageType;
                byte[] data = request.Data;

                CacheHelper.LogTrace(
                    "ProcessMessage type: {0}, numBytes: {1}", 
                    t.ToString(), 
                    data == null ? "null" : data.Length.ToString());

                switch (t)
                {
                    case CacheRequestTypes.GetConfig:        return GetConfig(data);
                    case CacheRequestTypes.NodeDown:         return NodeDown(data);
                    case CacheRequestTypes.AddNode:          return AddNode(data);
                    case CacheRequestTypes.RemoveNode:       return RemoveNode(data);
                    case CacheRequestTypes.ChangeNode:       return ChangeNode(data);
                    case CacheRequestTypes.GetStats:         return GetStats(data);
                    case CacheRequestTypes.GetObject:        return GetObject(data);
                    case CacheRequestTypes.PutObject:        return PutObject(data);
                    case CacheRequestTypes.DeleteObject:     return DeleteObject(data);
                    case CacheRequestTypes.ChangeConfig:     return ChangeConfig(data);
                    case CacheRequestTypes.Register:        
                        return Register(data, request.ClientEndPoint);

                    default: 
                        CacheMessage response = new CacheMessage();
                        response.MessageType = (byte)CacheResponseTypes.InvalidRequestType;
                        response.Data = new byte[] {};
                        return response;
                }
            }
            catch (CacheMessageException ex)
            {
                // This exception is usually caused by the client doing something wrong
                CacheHelper.LogError(ex.ToString());
                CacheMessage response = new CacheMessage();
                response.MessageType = ex.ResponseType;
                return response;
            }
            catch (Exception ex)
            {
                // Anything that causes this exception is a bug we need to fix
                CacheHelper.LogError(ex.ToString());
                CacheMessage response = new CacheMessage();
                response.MessageType = 
                    (byte)CacheResponseTypes.InternalServerError;
                return response;
            }
        }

        /// <summary>A request for the node's current configuration</summary>
        /// <remarks>This method returns all node locations, which makes it 
        /// appropriate for clients that can't calculate the ring.  The data
        /// size can be somewhat large for a large cluster.</remarks>
        public CacheMessage GetConfig(byte[] data)
        {
            CacheMessage response = new CacheMessage();

            response.MessageType = (byte)CacheResponseTypes.Configuration;
            response.Data = SerializeNodes(this.config.Ring, true);

            return response;
        }

        /// <summary>A report from a client that a node is not responding</summary>
        public CacheMessage NodeDown(byte[] data)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(
                        CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage();

            // TODO

            return response;
        }

        /// <summary>A request for the master node to add a data node and push the
        /// change out to all data nodes.</summary>
        /// <remarks>This request comes from the administrative console, and it 
        /// should be issued after the data node has been started.  The data
        /// node will be trying to register with the master, with a longer and 
        /// longer retry, so if you wait too long, it will be a while before
        /// the data node starts listening.  Not good.  TODO</remarks>
        public CacheMessage AddNode(byte[] data)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage();

            CacheNode newNode = null;
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader br = new BinaryReader(ms))
                {
                    newNode = DeserializeNode(br);
                }
            }

            // First make sure we don't already have this node
            CacheNode already = this.config.Ring.FindNodeByName(newNode.GetName());

            if (already != null)
                throw new CacheMessageException(CacheResponseTypes.NodeExists);

            // Add it to the ring
            this.config.Ring.AddNode(newNode); 

            // Iterate through data nodes and send them all the new config
            // (except the one we just added which will get it when it registers)
            // TODO

            return response;
        }

        /// <summary>A request for the master node to remove a data node and push the
        /// change out to all data nodes.</summary>
        public CacheMessage RemoveNode(byte[] data)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage();

            // TODO

            return response;
        }

        /// <summary>
        /// A request for the master node to make a change to a node and push the
        /// change out to all data nodes
        /// </summary>
        public CacheMessage ChangeNode(byte[] data)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

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

            // Binary request format:
            //
            // KeyLen    int (The length of the packet, which we already got)
            // Key        byte[] UTF8 string

            if (!this.config.IsDataNode)
                throw new CacheMessageException(CacheResponseTypes.NotDataNode);

            string keyString = null;
            
            try
            {
                keyString = Encoding.UTF8.GetString(data);
            }
            catch (Exception)
            {
                // The client sent something that's not a string
                throw new CacheMessageException(CacheResponseTypes.ReadKeyError);
            }

            // If this node owns the key, return the object.  
            // If not, tell the client to reconfigure.
            if (IsThisNode(keyString))
                return GetObject(keyString);
            else
                return CreateReConfigureMessage();
        }

        private CacheMessage GetObject(string keyString)
        {
            // This method is called after we have determined the this node owns the key
            try
            {
                this.dataLock.EnterReadLock();

                CacheMessage response = new CacheMessage();
                
                byte[] data;
                if (this.dataByKey.TryGetValue(keyString, out data))
                {
                    response.MessageType = 
                        (byte)CacheResponseTypes.ObjectOk;
                    response.Data = data;
                }
                else
                {
                    response.MessageType = 
                        (byte)CacheResponseTypes.ObjectMissing;
                }
                return response;
            }
            finally
            {
                this.dataLock.ExitReadLock();
            }
        }

        /// <summary>Serializes the config for clients and other nodes</summary>
        private byte[] SerializeNodes(CacheRing ring, bool includeLocations)
        {
            MemoryStream ms = new MemoryStream();

            using (BinaryWriter w = new BinaryWriter(ms))
            {
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

                w.Write(ToNetwork(ring.Nodes.Count));
                foreach (var node in ring.Nodes.Values)
                {
                    byte[] hostNameData = GetBytes(node.HostName);
                    w.Write(ToNetwork(hostNameData.Length));
                    w.Write(hostNameData);
                    w.Write(ToNetwork(node.PortNumber));
                    w.Write(ToNetwork(node.MaxNumBytes));

                    if (includeLocations)
                    {
                        w.Write(ToNetwork(node.Locations.Count));
                        foreach (var location in node.Locations)
                        {
                            w.Write(ToNetwork(location));
                        }
                    }
                    else
                    {
                        w.Write(ToNetwork((int)0));
                    }
                }
                w.Flush();
                return ms.ToArray();
            }
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

        /// <summary>
        /// A request from a client to a data node to store an object in the cache.
        /// </summary>
        public CacheMessage PutObject(byte[] messageData)
        {
            if (!this.config.IsDataNode)
                throw new CacheMessageException(CacheResponseTypes.NotDataNode);

            using (MemoryStream ms = new MemoryStream(messageData))
            {
                BinaryReader br = new BinaryReader(ms);

                string keyString = ReadKey(br);

                byte[] objectData;
                if (!TryReadArray(br, out objectData))
                    throw new CacheMessageException(CacheResponseTypes.ReadDataError);

                // If this node owns the key, store the object.
                // If not, tell the client to reconfigure.
                if (IsThisNode(keyString))
                    return PutObject(keyString, objectData);
                else
                    return CreateReConfigureMessage();
            }
        }

        private CacheMessage PutObject(string key, byte[] data)
        {
            // This method is called after we have determined that this node owns the key

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

                    // We have to clear out any stale put times for
                    // this key
                    if (this.keysByTime.ContainsKey(oldDateTime))
                    {
                        List<string> keysAtOldTime = 
                            this.keysByTime[oldDateTime];
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

        /// <summary>Returns true if this is the node specified.</summary>
        private bool IsThisNode(CacheNode node)
        {
            if (node.HostName.Equals(this.config.ListenerHostName) && 
                node.PortNumber.Equals(this.config.ListenerPortNumber))
            {
                return true;
            }
            return false;
        }

        /// <summary>Returns true if this node owns the specified key.</summary>
        /// <remarks>This method tries once, and if it doesn't own the key, 
        /// it goes to the master for updated configuration, just in case the 
        /// cluster has changed.  If the cluster has changed, a rebalance is 
        /// started in the background.</remarks>
        private bool IsThisNode(string keyString)
        {
            // Hash the key, make sure this node owns it
            int hash = CacheHelper.GetConsistentHashCode(keyString);
            CacheNode node = this.config.Ring.GetNodeForHash(hash);
            if (IsThisNode(node))
            {
                return true;
            }
            else
            {
                // A client asked this node to store an object that doesn't
                // match up with this node's virtual node locations.  Since
                // clients and nodes cache the ring, there might have been
                // changes since we started, so let's check with master to see
                // if the config changed.

                // Check with master for possibly new config
                CacheRing ring = RegisterWithMaster();
                this.config.Ring = ring;

                // See if the node location changed
                node = this.config.Ring.GetNodeForHash(hash);

                if (IsThisNode(node))
                {
                    // This means the client had it right and this node had it
                    // wrong.  The master is supposed to tell us about changes,
                    // but sometimes we might hear it from a client first.
                    // Rabalance regardless.
                    StartRebalance();

                    return true;
                }
                else
                {
                    // This means the client is still wrong
                    return false;
                }
            }
        }

        /// <summary>Read a string from the reader</summary>
        private string ReadKey(BinaryReader br)
        {
            byte[] key;
            if (!TryReadArray(br, out key))
                throw new CacheMessageException(CacheResponseTypes.ReadKeyError);
            
            // Make sure the key is a string
            string keyString = null;
            try
            {
                keyString = Encoding.UTF8.GetString(key);
            }
            catch (Exception ex)
            {
                CacheHelper.LogTrace(ex.ToString());
                throw new CacheMessageException(CacheResponseTypes.ReadKeyError);
            }

            return keyString;
        }

        private CacheMessage CreateReadKeyErrorMessage()
        {
            CacheMessage response = new CacheMessage();
            response.MessageType = (byte)CacheResponseTypes.ReadKeyError;
            return response;
        }

        private CacheMessage CreateReConfigureMessage()
        {
            CacheMessage response = new CacheMessage();
            response.MessageType = (byte)CacheResponseTypes.ReConfigure;
            response.Data = SerializeNodes(this.config.Ring, true);
            return response;
        }

        /// <summary>Try to read {Length:int}{Data:byte[]} from the stream</summary>
        bool TryReadArray(BinaryReader r, out byte[] data)
        {
            try
            {
                int len = IPAddress.NetworkToHostOrder(r.ReadInt32());
                data = new byte[len];
                int bytesRead = r.Read(data, 0, len);
                if (bytesRead == len) 
                {
                    return true;
                }
                else 
                {
                    CacheHelper.LogTrace(
                        "TryReadArray expected {0} bytes, got {1}", 
                        len, bytesRead);
                    return false;
                }
            }
            catch (Exception ex)
            {
                CacheHelper.LogTrace(ex.ToString());
                data = new byte[0];
                return false;
            }
        }

        /// <summary>
        /// A request from a client to a data node to remove an object from the cache
        /// </summary>
        public CacheMessage DeleteObject(byte[] data)
        {
            if (!this.config.IsDataNode)
                throw new CacheMessageException(
                    CacheResponseTypes.NotDataNode);

            using (MemoryStream ms = new MemoryStream(data))
            {
                BinaryReader br = new BinaryReader(ms);

                string keyString = ReadKey(br);

                // If this node owns the key, remove the object.
                // If not, tell the client to reconfigure.
                if (IsThisNode(keyString))
                    return DeleteObject(keyString);
                else
                    return CreateReConfigureMessage();
            }
        }

        private CacheMessage DeleteObject(string keyString)
        {
            // This method is called after we have determined that this node owns the key
            try
            {
                this.dataLock.EnterWriteLock();

                bool inDataByKey = false;

                if (this.dataByKey.ContainsKey(keyString))
                {
                    inDataByKey = true;

                    // Remove the object from the main list
                    this.dataByKey.Remove(keyString);
                }

                DateTime keyPutTime;
                if (this.keyPutTimes.TryGetValue(keyString, out keyPutTime))
                {
                    if (!inDataByKey)
                    {
                        CacheHelper.LogTrace(
                        "{0} was in keyPutTimes but not dataByKey", 
                        keyString);
                    }

                    // Remove the reference to the put time
                    this.keyPutTimes.Remove(keyString);

                    List<string> keys;
                    if (this.keysByTime.TryGetValue(keyPutTime, out keys))
                    {
                        if (keys.Contains(keyString))
                        {
                            // Remove the key from the list at that put time
                            keys.Remove(keyString);
                        }
                        else
                        {
                            CacheHelper.LogTrace(
                                "{0} was not in keysByTime");
                        }
                    }
                }
                else
                {
                    if (inDataByKey)
                    {
                        CacheHelper.LogTrace(
                            "{0} was not in keyPutTimes", 
                            keyString);
                    }
                }

                CacheMessage response = new CacheMessage();

                if (inDataByKey)
                    response.MessageType = 
                        (byte)CacheResponseTypes.ObjectOk;
                else
                    response.MessageType = 
                        (byte)CacheResponseTypes.ObjectMissing;

                return response;
            }
            finally
            {
                this.dataLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// A request from the master to a data node to change its configuration.
        /// </summary>
        public CacheMessage ChangeConfig(byte[] data)
        {
            if (this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotDataNode);

            CacheMessage response = new CacheMessage();

            this.config.Ring = DeserializeNodes(data);

            return response;
        }

        /// <summary>
        /// A request from a data node on startup to the master, to let the 
        /// master know the data node is ready, and to get config from the master.
        /// </summary>
        /// <remarks>
        /// This can also be called to tell the master the data node is back up
        /// or to reload the configuration.  It doesn't hurt to call it multiple times.
        /// </remarks>
        public CacheMessage Register(byte[] data, IPEndPoint remoteEndPoint)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage();

            // All we need from the caller is the port number, since we can get 
            // the IP address from the TcpClient.  The remote end point port is 
            // the outgoing port, not the one the data node is listening on.
            int remoteListenerPort = -1;

            // Binary data format:
            //
            // PortNumber int
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader br = new BinaryReader(ms))
                {
                    remoteListenerPort = FromNetwork(br.ReadInt32());
                }
            }

            IPEndPoint remoteListenerIP = 
                new IPEndPoint(remoteEndPoint.Address, remoteListenerPort);

            var node = this.config.Ring.FindNodeByIP(remoteListenerIP);
            if (node == null)
            {
                // Data node is missing from the master's config file
                
                CacheHelper.LogTrace(
                    "Node {0} tried to register, but it's not configured", 
                    remoteListenerIP.ToString());

                throw new CacheMessageException(CacheResponseTypes.UnknownNode);
            }
            
            node.Status = CacheNodeStatus.Up;

            CacheHelper.LogTrace("Node {0} registered", remoteListenerIP.ToString());

            // Send configuration
            response.MessageType = (byte)CacheResponseTypes.Configuration;
            response.Data = SerializeNodes(this.config.Ring, false);
            return response;
        }

        /// <summary>
        /// Write a response to the network stream.
        /// </summary>
        /// <param name="n"></param>
        /// <param name="responseType"></param>
        /// <param name="responseData"></param>
        /// <returns></returns>
        async Task WriteResponse(NetworkStream n, 
            byte responseType, byte[] responseData)
        {
            await Task.Yield();

            CacheHelper.LogTrace("WriteResponse type: {0}, numBytes: {1}", 
                responseType.ToString(), responseData == null ? "null" : 
                    responseData.Length.ToString());

            BinaryWriter w = new BinaryWriter(n);
            w.Write(responseType);
            int length = responseData == null ? 0 : responseData.Length;
            w.Write(IPAddress.HostToNetworkOrder(length));
            if (responseData != null && responseData.Length > 0)
            {
                w.Write(responseData);
            }
            w.Flush();
        }

        /// <summary>
        /// Read the complete client request from the stream and deserialize it.
        /// </summary>
        /// <remarks>This is an async method so it might return control to the caller
        /// while it waits for bytes to come across the network.</remarks>
        /// <param name="n"></param>
        /// <returns></returns>
        private async Task<CacheMessage> GetRequestFromNetworkStream(NetworkStream n)
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
                    await n.ReadAsync(    
                        data, 
                        bytesRead, 
                        data.Length - bytesRead);
            }

            CacheMessage m = new CacheMessage();
            m.MessageType = messageType;
            m.Data = data;

            return m;
        }

        private CacheNode DeserializeNode(BinaryReader reader)
        {
            CacheNode node = new CacheNode();
            int hostLen = FromNetwork(reader.ReadInt32());
            byte[] hostData = new byte[hostLen];
            if (reader.Read( hostData, 0, hostLen) != hostLen)
            {
                throw new Exception(
                    "data shorter than expected");
            }
            node.HostName = Encoding.UTF8.GetString(hostData);
            node.PortNumber = FromNetwork(reader.ReadInt32());
            node.MaxNumBytes = FromNetwork(reader.ReadInt64());
            int numLocations = FromNetwork(reader.ReadInt32());
            if (numLocations > 0)
            {
                for (int i = 0; i < numLocations; i++)
                {
                    int location = FromNetwork(reader.ReadInt32());
                    node.Locations.Add(location);
                }
            }
            return node;
        }

        /// <summary>Deserialize nodes</summary>
        /// <remarks>If node locations are not pre-populated, this method
        /// will also determine node locations for the ring before returning</remarks>
        private CacheRing DeserializeNodes(byte[] data)
        {
            CacheRing ring = new CacheRing();

            bool hasLocations = true;

            // Deserialize the config
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(ms))
                {
                    int numNodes = FromNetwork(reader.ReadInt32());
                    for (int n = 0; n < numNodes; n++)
                    {
                        var node = DeserializeNode(reader);
                        if (node.Locations.Count == 0)
                        {
                            hasLocations = false;
                        }
                        foreach (var location in node.Locations)
                        {
                            ring.SortedLocations.Add(
                                location, 
                                node);
                        }
                        ring.Nodes.Add(node.GetName(), node);
                    }
                }
            }

            if (!hasLocations)
            {
                // Data nodes will get just the basic node configurations,
                // since they can determine the node locations consistently.
                
                ring.PopulateNodes();
            }

            return ring;
        }

        /// <summary>Register with the master and get the cache ring</summary>
        /// <remarks>Master returns a bare configuration, so this method will 
        /// do DNS lookups and determine the virtual node locations before returning.
        /// </remarks>
        private CacheRing RegisterWithMaster()
        {
            if (this.config.IsMaster && this.config.IsDataNode)
            {
                // It's not necessary to register if this is both a master
                // and a data node
                return this.config.Ring;
            }

            CacheMessage request = new CacheMessage();
            request.MessageType = (byte)CacheRequestTypes.Register;
            using (MemoryStream ms = new MemoryStream())
            {
                using (BinaryWriter bw = new BinaryWriter(ms))
                {
                    bw.Write(ToNetwork(this.config.ListenerPortNumber));
                    bw.Flush();
                    request.Data = ms.ToArray();
                }
            }

            try
            {
                CacheMessage response = CacheHelper.SendRequest(request, 
                    this.config.MasterIPEndPoint);
            
                if (response.MessageType == (byte)CacheResponseTypes.Configuration)
                {
                    CacheRing ring = DeserializeNodes(response.Data);
                    return ring;
                }

                // TODO - We'll get an unknown node response during the time
                // between when a new data node is started and when a message is 
                // sent to the master to add the node to the cluster.
                if (response.MessageType == (byte)CacheResponseTypes.UnknownNode)
                {
                    CacheHelper.LogTrace("Tried to register with master, got UnknownNode");
                    return null;
                }

                CacheHelper.LogTrace("Got an unexpected response from master: {0}", 
                    response.MessageType);

                // Not sure what to do here.  Doing nothing will result in a retry.
                // But an unexpected response might mean it will never succeed.
                // TODO
            }
            catch (SocketException)
            {
                // We expect this if the master isn't up yet
                CacheHelper.LogTrace("Got a SocketException from master, " +
                    "assuming it's probably not up yet");
            }

            CacheHelper.LogError("Unable to register with master");

            // The caller should keep trying until the master responds, since
            // it's possible the cluster is being brought up all at once, 
            // or the master is down for some reason.  A data node can't 
            // go any further without registering with the master.

            return null;
        }

        /// <summary>Stop the listener</summary>
        public void Stop()
        {
            this.shouldRun = false;
            try
            {
                this.listener.Stop();
            }
            catch {}
        }
        
        /// <summary>
        /// Pause execution until the specified DateTime, unless shouldRun is set
        /// to false.
        /// </summary>
        /// <param name="pauseUntil"></param>
        /// <param name="pauseIncrementMs"></param>
        /// <returns></returns>
        private bool StoppablePause(DateTime pauseUntil, int pauseIncrementMs)
        {
            while (this.shouldRun)
            {
                if (DateTime.UtcNow < pauseUntil) 
                    Thread.Sleep(pauseIncrementMs);
                else break;
            }

            return this.shouldRun;
        }

        /// <summary>
        /// Pause execution until the specified DateTime, unless shouldRun is set
        /// to false.
        /// </summary>
        /// <param name="pauseUntil"></param>
        /// <returns></returns>
        private bool StoppablePause(DateTime pauseUntil)
        {
            return StoppablePause(pauseUntil, 500);
        }

        /// <summary>
        /// Pause execution for the specified length of time, unless shouldRun is
        /// set to false.
        /// </summary>
        /// <param name="pauseDuration"></param>
        /// <returns></returns>
        private bool StoppablePause(TimeSpan pauseDuration)
        {
            return StoppablePause(DateTime.UtcNow + pauseDuration, 500);
        }

        /// <summary>
        /// Pause execution for the specified number of seconds, 
        /// unless shouldRun is set to false.
        /// </summary>
        /// <param name="pauseSeconds"></param>
        /// <returns></returns>
        private bool StoppablePause(int pauseSeconds)
        {
            return StoppablePause(DateTime.UtcNow.AddSeconds(pauseSeconds), 500);
        }

        int ToNetwork(int i)
        {
            return IPAddress.HostToNetworkOrder(i);
        }

        int FromNetwork(int i)
        {
            return IPAddress.NetworkToHostOrder(i);
        }

        long ToNetwork(long i)
        {
            return IPAddress.HostToNetworkOrder(i);
        }

        long FromNetwork(long i)
        {
            return IPAddress.NetworkToHostOrder(i);
        }

        byte[] GetBytes(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        string GetString(byte[] b)
        {
            return Encoding.UTF8.GetString(b);
        }

    }

}

