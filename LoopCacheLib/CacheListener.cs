using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LoopCacheLib
{
    /// <summary>
    /// Listens on a port for cache requests from clients
    /// </summary>
    /// <remarks>
    /// Depending on configuration, can be a master or data node
    /// </remarks>
    public class CacheListener
    {
        // Remember: FAST FAST FAST

        /// <summary>
        /// This is the object cache
        /// </summary>
        private Dictionary<string, byte[]> dataByKey;

        /// <summary>
        /// A list of keys sorted by put time
        /// </summary>
        /// <remarks>
        /// Each datetime is a bucket of keys, since more than one may be
        /// saved at the same time.  
        /// </remarks>
        private SortedList<DateTime, List<string>> keysByTime;

        /// <summary>
        /// The last time objects were saved
        /// </summary>
        private Dictionary<string, DateTime> keyPutTimes;

        //private Thread putThread;
        //private ObservableCollection<KeyValuePair<string, byte[]>> puts;

        /// <summary>
        /// The total number of bytes in dataByKey
        /// </summary>
        private long totalDataBytes;

        /// <summary>
        /// The total number of bytes in WorkingSet64 the last time we checked
        /// </summary>
        private long latestRAMBytes;

        /// <summary>
        /// The relationship of the total data bytes to actual RAM usage, 
        /// based on the last time we checked the RAM usage.
        /// </summary>
        private double ramMultiplier;

        /// <summary>
        /// Synchronizes access to the data in the cache
        /// </summary>
        private ReaderWriterLockSlim dataLock = new ReaderWriterLockSlim();

        /// <summary>
        /// Configuration for this listener.
        /// </summary>
        private CacheConfig config;

        /// <summary>
        /// The location of the config file
        /// </summary>
        /// <remarks>
        /// The listener can edit the file if it's the master
        /// </remarks>
        private string configFilePath;

        /// <summary>
        /// The IP and Port that we're listening on
        /// </summary>
        private IPEndPoint ipep;

        /// <summary>
        /// If set to false, stop accepting new client requests
        /// </summary>
        private bool shouldRun;

        /// <summary>
        /// The TCP Listener
        /// </summary>
        private TcpListener listener;

        /// <summary>
        /// If this listener is a data node, a reference to this node in the ring.
        /// </summary>
        private CacheNode localDataNode;

        /// <summary>
        /// Create an uninitialized instance of the listener
        /// </summary>
        /// <remarks>
        /// This constructor is for testing
        /// </remarks>
        public CacheListener()
        {
            this.dataByKey = new Dictionary<string, byte[]>();
            this.keysByTime = new SortedList<DateTime, List<string>>();
            this.keyPutTimes = new Dictionary<string, DateTime>();
            //this.puts = new List<KeyValuePair<string, byte[]>>();

            //this.putThread = new Thread(PutObjectWorker);
            //this.putThread.Start();

            //this.puts = new ObservableCollection<KeyValuePair<string, byte[]>>();
            //this.puts.CollectionChanged += puts_CollectionChanged;
        }

        /// <summary>
        /// Create a new listener based on a config file
        /// </summary>
        /// <param name="configFilePath"></param>
        public CacheListener(string configFilePath) : this(CacheConfig.Load(configFilePath))
        {
            this.configFilePath = configFilePath;
        }

        /// <summary>
        /// Create a new listener based on a provided config
        /// </summary>
        public CacheListener(CacheConfig config) : this()
        {
            this.config = config;

            if (this.config.IsTraceEnabled) 
            {
                CacheHelper.TraceFilePath = config.TraceFilePath;
            }

            CacheHelper.LogTrace("Starting listener with config settings: \r\n{0}",
                this.config.GetTrace());

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

            if (this.configFilePath == null)
            {
                string configFile = string.Format("CacheConfig_{0}.txt", this.config.ListenerPortNumber);
                string path = System.Reflection.Assembly.GetExecutingAssembly().Location;
                string dir = System.IO.Path.GetDirectoryName(path);
                this.configFilePath = Path.Combine(dir, configFile);
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

            // Start the background thread that monitors RAM usage
            new Thread(new ThreadStart(MonitorRAM)).Start();

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
                    var task = Accept(await listener.AcceptTcpClientAsync());
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
            {
                InitializeMaster();
            }
            else
            {
                // We need a data node to start listening immediately, so that
                // the master can communicate with it before it has even registered.
                // Initialize on a background thread and send DataNodeNotReady for 
                // all requests until the ring config has been received from the master.

                Thread t = new Thread(InitializeDataNode);
                t.Start();
            }
        }

        private void InitializeMaster()
        {
            // Don't bother with a DNS lookup for the master endpoint
            this.config.MasterIPEndPoint = this.ipep;
        }

        private void InitializeDataNode()
        {
            try
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

                        int pauseSeconds = 1; 

                        // Originally we had incremental backoff here, with the pauseSeconds
                        // as the number of tries, but when a data node is brought up 
                        // for the first time and added to a running cluster, it will
                        // keep trying to register until an admin adds it to the cluster config, 
                        // which might take a while.  We want the node to become operational
                        // as soon as possible after the master node gets the Add message.

                        this.StoppablePause(pauseSeconds);
                    }
                    else
                    {
                        CacheHelper.LogTrace("Successfully registered with master");
                        CacheHelper.LogTrace("Config:\r\n{0}", this.config.Ring.GetTrace());

                        // Store a reference to the local data node
                        foreach (CacheNode node in this.config.Ring.Nodes.Values)
                        {
                            if (IsThisNode(node))
                            {
                                this.localDataNode = node;
                                break;
                            }
                        }

                        if (this.localDataNode == null)
                        {
                            // This should never happen
                            throw new Exception(
                                "RegisterWithMaster succeeded, but there is no local node");
                        }
                    }

                } while (this.config.Ring == null && this.shouldRun);
            }
            catch (Exception ex)
            {
                // We can't continue if something fails here.  Shut down.
                Stop();
                CacheHelper.LogError("Unable to register", ex);
            }
        }

        /// <summary>
        /// Accept a new client TCP request.
        /// </summary>
        /// <remarks>This is an async method so it returns control back to the caller 
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

                        // Set the end point in case we need to validate it later.
                        request.ClientEndPoint = (IPEndPoint)client.Client.RemoteEndPoint;

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

                if (this.config.Ring == null)
                {
                    // This is a data node and it's still trying to register with 
                    // the master, so we need to tell callers we're not ready.
                    // The master node will use this response as a way to confirm
                    // that the data node is up and running, before it adds the
                    // node to the ring.
                    return new CacheMessage(CacheResponseTypes.DataNodeNotReady);
                }

                switch (t)
                {
                    case CacheRequestTypes.GetConfig:        return GetConfig(data);
                    case CacheRequestTypes.NodeUnreachable:  return NodeUnreachable(data);
                    case CacheRequestTypes.AddNode:          return AddNode(data);
                    case CacheRequestTypes.RemoveNode:       return RemoveNode(data);
                    case CacheRequestTypes.ChangeNode:       return ChangeNode(data);
                    case CacheRequestTypes.GetStats:         return GetStats(data);
                    case CacheRequestTypes.GetObject:        return GetObject(data);
                    case CacheRequestTypes.PutObject:        return PutObject(data);
                    case CacheRequestTypes.DeleteObject:     return DeleteObject(data);
                    case CacheRequestTypes.ChangeConfig:     return ChangeConfig(data);
                    case CacheRequestTypes.Ping:             return Ping();
                    case CacheRequestTypes.Register:        
                        return Register(data, request.ClientEndPoint);
                    case CacheRequestTypes.FireSale:         return FireSale(request.ClientEndPoint);
                    case CacheRequestTypes.Clear:            return Clear();

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

        public CacheMessage Clear()
        {
            //
            //  Only master can recieve clear.
            //
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage();
            response.MessageType = (byte)CacheResponseTypes.Accepted;

            Thread t = new Thread(BackgroundClear);
            t.Start();

            return response;
        }

        private void BackgroundClear()
        {
            //
            //  Call FireSale on all nodes.
            //
            var nodeEndPoints = this.config.Ring.GetNodeEndPoints();

            Parallel.ForEach(nodeEndPoints, node =>
            {
                int maxNumTries = 3;
                int numTries = 0;

                do
                {
                    try
                    {
                        numTries++;

                        CacheMessage request = new CacheMessage();
                        request.MessageType = (byte)CacheRequestTypes.FireSale;

                        CacheMessage response = CacheHelper.SendRequest(request, node.Value);

                        if (response == null || response.MessageType != (byte)CacheResponseTypes.Accepted)
                        {
                            //
                            //  What to do?
                            //
                        }
                    }
                    catch (Exception)
                    {
                        //
                        //  What to do?
                        //
                    }
                }
                while (numTries < maxNumTries);
            });
        }

        public CacheMessage FireSale(IPEndPoint remoteEndPoint)
        {
            //
            //  Only data nodes have data to throw away.
            //
            if (!this.config.IsDataNode)
                throw new CacheMessageException(CacheResponseTypes.NotDataNode);
            //
            //  Only master can call FireSale!
            //
            //if (!remoteEndPoint.Equals(this.config.MasterIPEndPoint))
            //    throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage();
            response.MessageType = (byte)CacheResponseTypes.Accepted;

            Thread t = new Thread(BackgroundFireSale);
            t.Start();

            return response;
        }

        private void BackgroundFireSale()
        {
            this.dataByKey.Clear();
            this.keysByTime.Clear();
            this.keyPutTimes.Clear();
            this.totalDataBytes = 0;
            this.UpdateRAMStats();
        }

        /// <summary>
        /// A request for the node's current configuration
        /// </summary>
        /// <remarks>
        /// This method returns all node locations, which makes it 
        /// appropriate for clients that can't calculate the ring.  The data
        /// size can be somewhat large for a large cluster.
        /// </remarks>
        public CacheMessage GetConfig(byte[] data)
        {
            CacheMessage response = new CacheMessage();

            response.MessageType = (byte)CacheResponseTypes.Configuration;
            response.Data = SerializeNodes(this.config.Ring, true);

            return response;
        }

        /// <summary>
        /// A report from a client that a node is not responding
        /// </summary>
        public CacheMessage NodeUnreachable(byte[] data)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage(CacheResponseTypes.Accepted);

            //  MessageType     byte (2)
            //  DataLength      int
            //  Data            byte[]
            //      HostLen         int
            //      Host            byte[] UTF8 string
            //      Port            int

            string nodeName = null;
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(ms))
                {
                    int hostLen = FromNetwork(reader.ReadInt32());
                    byte[] hostData = new byte[hostLen];
                    if (reader.Read(hostData, 0, hostLen) != hostLen)
                    {
                        throw new Exception("data shorter than expected");
                    }
                    string hostName = Encoding.UTF8.GetString(hostData);
                    int portNumber = FromNetwork(reader.ReadInt32());
                    nodeName = CacheNode.CreateName(hostName, portNumber);
                }
            }

            if (nodeName == null)
                return new CacheMessage(CacheResponseTypes.UnknownNode);

            CacheNode node = this.config.Ring.FindNodeByName(nodeName);

            if (node == null)
                return new CacheMessage(CacheResponseTypes.UnknownNode);
            //
            //  Ping the node, 
            //
            var pingResponse = this.PingNode(node);
            //
            //  if it doesnt respond set it to Questionable call interpol.
            //
            if (pingResponse == null)
            {
                this.config.Ring.SetNodeStatus(node, CacheNodeStatus.Questionable);
                //
                //  TODO: Need to do something like email admin.
                //
            }
            else
            {
                return new CacheMessage(CacheResponseTypes.NodeExists);
            }

            return response;
        }

        /// <summary>
        /// A request for the master node to add a data node and push the
        /// change out to all data nodes.
        /// </summary>
        /// <remarks>
        /// This request comes from the administrative console, and it 
        /// should be issued after the data node process/service has been started.  
        ///     HostLen         int
        ///     Host            byte[] UTF8 string
        ///     Port            int
        ///     MaxNumBytes     long
        ///     Status          byte
        /// </remarks>
        public CacheMessage AddNode(byte[] data)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage();
            response.MessageType = (byte)CacheResponseTypes.Accepted;

            CacheNode newNode = null;
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader br = new BinaryReader(ms))
                {
                    try
                    {
                        newNode = DeserializeNode(br);
                    }
                    catch (Exception dex)
                    {
                        CacheHelper.LogError("Unable to deserialize", dex);
                        throw new CacheMessageException(CacheResponseTypes.ReadDataError);
                    }
                }
            }

            // Make sure we don't already have this node
            CacheNode already = this.config.Ring.FindNodeByName(newNode.GetName());

            if (already != null)
                throw new CacheMessageException(CacheResponseTypes.NodeExists);

            // Make sure DNS resolves
            newNode.IPEndPoint = CacheHelper.GetIPEndPoint(newNode.HostName, newNode.PortNumber);

            var pingResponse = this.PingNode(newNode);
            if (pingResponse == null)
            {
                CacheHelper.LogWarning("Unexpected null response from added data node");
                throw new CacheMessageException(CacheResponseTypes.InternalServerError);
            }

            byte[] okResponses = new byte[]{
                (byte)CacheResponseTypes.DataNodeNotReady,
                (byte)CacheResponseTypes.Accepted
            };

            byte messageType = pingResponse.MessageType;

            if (messageType != okResponses[0] && messageType != okResponses[1])
            {
                string accepted = CacheResponseTypes.Accepted.ToString();
                string datanodenotready = CacheResponseTypes.DataNodeNotReady.ToString();
                string received = ((CacheResponseTypes)pingResponse.MessageType).ToString();

                CacheHelper.LogWarning(
                    string.Format( 
                        "Unexpected response {0} or {1} but received {2} from added data node",
                        accepted,
                        datanodenotready,
                        received
                    )
                );
                throw new CacheMessageException(CacheResponseTypes.InternalServerError); 
            }

            // Now we know that the data node was started and it's trying to register.

            // Spawn a background thread to do this and return to 
            // the caller immediately.  Push to nodes in parallel and 
            // retry a few times if a node doesn't respond.  Mark a node as 
            // questionable if it doesn't respond.

            Thread t = new Thread(BackgroundAddNode);
            t.Start(newNode);

            return response;
        }

        private void BackgroundAddNode(object newNodeObj)
        {
            try
            {
                CacheNode newNode = newNodeObj as CacheNode;

                newNode.Status = CacheNodeStatus.Up;

                // Add it to the ring
                this.config.Ring.AddNode(newNode);

                CacheHelper.LogTrace("Added node {0}.  New config:\r\n{1}",
                    newNode.IPEndPoint, this.config.Ring.GetTrace());

                // Save edits to the config file so the node persists if we restart master
                CacheConfig.Save(this.configFilePath, this.config);

                // Iterate through data nodes and send them all the new config
                var nodeEndPoints = this.config.Ring.GetNodeEndPoints();

                // Create a list of end points that need the configuration
                var endPointsToPush = new Dictionary<string, IPEndPoint>();

                if (IsThisNodeEndPoint(newNode.IPEndPoint))
                {
                    //
                    //  The node changed is the master. Update it, but dont push.
                    //                 
                    this.localDataNode = newNode;
                }

                foreach (var kvp in nodeEndPoints)
                {
                    // Skip self if master is also a data node
                    if (IsThisNodeEndPoint(kvp.Value))
                        continue;

                    endPointsToPush.Add(kvp.Key, kvp.Value);
                }
                //
                //  Sometimes only the master is there and its also the data node.
                //
                if ( endPointsToPush.Count > 0 )
                    this.PushConfigToNodes(endPointsToPush);
            }
            catch (Exception ex)
            {
                // We're on a background thread here so there's nothing we can do 
                // to let the caller know, since we returned a response already.
                CacheHelper.LogError("Unable to add node", ex);
            }
        }

        private void PushConfigToNodes(Dictionary<string, IPEndPoint> endPointsToPush)
        {
            // Serialize the current node configuration
            byte[] config = SerializeNodes(this.config.Ring, false);

            CacheHelper.LogTrace("About to push config to {0} nodes", endPointsToPush.Count);

            // Send messages to all nodes in parallel
            Parallel.ForEach(endPointsToPush, kvp =>
            {
                bool success = PushConfigToNode(kvp.Value, config);

                if (!success)
                {
                    // Mark the node as questionable.  It might be down.
                    this.config.Ring.SetNodeStatus(kvp.Key, CacheNodeStatus.Questionable);

                    CacheHelper.LogTrace("Unable to push new config to node {0}", kvp.Value);
                }
                else
                {
                    CacheHelper.LogTrace("Pushed new config to node {0}", kvp.Value);
                }
            });
        }

        /// <summary>Push the serialized configuration to the specified node</summary>
        private bool PushConfigToNode(IPEndPoint nodeEndPoint, byte[] config)
        {
            int maxNumTries = 3;
            int numTries = 0;

            do
            {
                try
                {
                    numTries++;

                    CacheMessage request = new CacheMessage();
                    request.MessageType = (byte)CacheRequestTypes.ChangeConfig;
                    request.Data = config;
                    
                    CacheMessage response = CacheHelper.SendRequest(request, nodeEndPoint);

                    if (response.MessageType == (byte)CacheResponseTypes.Accepted)
                    {
                        return true;
                    }
                    else
                    {
                        // Why would we get a different response?
                        CacheHelper.LogError(string.Format(
                            "Got unexpected response {0} trying to push config to node {1}", 
                            response.MessageType, nodeEndPoint.ToString()));
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    CacheHelper.LogError(string.Format(
                        "Failed trying to push config to node {0}", nodeEndPoint.ToString()), ex);
                }
            }
            while (numTries < maxNumTries);

            return false;
        }

        /// <summary>A request for the master node to remove a data node and push the
        /// change out to all data nodes.</summary>
        public CacheMessage RemoveNode(byte[] data)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage();
            response.MessageType = (byte)CacheResponseTypes.Accepted;

            CacheNode nodeToRemove = null;
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader br = new BinaryReader(ms))
                {
                    int hostLen = FromNetwork(br.ReadInt32());
                    byte[] hostData = new byte[hostLen];
                    if (br.Read(hostData, 0, hostLen) != hostLen)
                    {
                        throw new Exception(
                            "data shorter than expected");
                    }
                    string hostName = Encoding.UTF8.GetString(hostData);
                    int portNumber = FromNetwork(br.ReadInt32());
                    nodeToRemove = this.config.Ring
                        .FindNodeByName(CacheNode.CreateName(hostName, portNumber));
                }
            }

            if (nodeToRemove == null)
            {
                return new CacheMessage(CacheResponseTypes.UnknownNode);
            }

            Thread t = new Thread(BackgroundRemoveNode);
            t.Start(nodeToRemove);

            return response;
        }

        private void BackgroundRemoveNode(object nodeToRemoveObj)
        {
            try
            {
                CacheNode nodeToRemove = nodeToRemoveObj as CacheNode;

                this.config.Ring.RemoveNodeByName(nodeToRemove.GetName());

                CacheHelper.LogTrace("Removed node {0}.  New config:\r\n{1}",
                    nodeToRemove.IPEndPoint, this.config.Ring.GetTrace());

                // Save edits to the config file so the node persists if we restart master
                CacheConfig.Save(this.configFilePath, this.config);

                // Iterate through data nodes and send them all the new config
                var nodeEndPoints = this.config.Ring.GetNodeEndPoints();

                // Create a list of end points that need the configuration
                var endPointsToPush = new Dictionary<string, IPEndPoint>();

                foreach (var kvp in nodeEndPoints)
                {
                    // Skip self if master is also a data node
                    if (IsThisNodeEndPoint(kvp.Value))
                        continue;

                    if (!endPointsToPush.ContainsKey(kvp.Key))
                        endPointsToPush.Add(kvp.Key, kvp.Value);

                    // Also push to the removed node in case it's still 
                    // listening so it knows it's been marked down.
                    if (!endPointsToPush.ContainsKey(nodeToRemove.GetName()))
                        endPointsToPush.Add(nodeToRemove.GetName(), nodeToRemove.IPEndPoint);
                }

                PushConfigToNodes(endPointsToPush);
            }
            catch (Exception ex)
            {
                // We're on a background thread here so there's nothing we can do 
                // to let the caller know, since we returned a response already.
                CacheHelper.LogError("Unable to remove node", ex);
            }
        }

        /// <summary>
        /// This request has no data, it's just a check to see if this node is listening
        /// </summary>
        public CacheMessage Ping()
        {
            CacheMessage response = new CacheMessage(CacheResponseTypes.Accepted);
            return response;
        }

        private CacheMessage PingNode(CacheNode node)
        {
            IPEndPoint ipep = CacheHelper.GetIPEndPoint(node.HostName, node.PortNumber);
            CacheMessage ping = new CacheMessage(CacheRequestTypes.Ping);
            CacheMessage response = null;

            try
            {
                response = CacheHelper.SendRequest(ping, ipep);
            }
            catch (SocketException)
            {
            }

            return response;
        }

        /// <summary>
        /// A request for the master node to make a change to a node and push the
        /// change out to all data nodes
        /// </summary>
        /// <remarks>Currently, the only thing that can change is the allocated number 
        /// of bytes.  This still requires pushing config out to all nodes, since
        /// it affects the ring locations.</remarks>
        public CacheMessage ChangeNode(byte[] data)
        {
            if (!this.config.IsMaster)
                throw new CacheMessageException(CacheResponseTypes.NotMasterNode);

            CacheMessage response = new CacheMessage(CacheResponseTypes.Accepted);

            CacheNode changedNode = null;
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader br = new BinaryReader(ms))
                {
                    changedNode = DeserializeNode(br);
                }
            }

            if (changedNode.MaxNumBytes <= 0)
            {
                CacheHelper.LogWarning(string.Format(
                    "Tried to change node {0} with MaxNumBytes {1}", changedNode.GetName(),
                    changedNode.MaxNumBytes));

                return new CacheMessage(CacheResponseTypes.InvalidConfiguration);
            }

            CacheNode actualNode = this.config.Ring.FindNodeByName(changedNode.GetName());

            if (actualNode == null)
            {
                return new CacheMessage(CacheResponseTypes.UnknownNode);
            }

            // I think we can get away with doing this outside of a lock
            actualNode.MaxNumBytes = changedNode.MaxNumBytes;

            Thread t = new Thread(BackgroundChangeNode);
            t.Start(actualNode);

            return response;
        }

        private void BackgroundChangeNode(object newNodeObj)
        {
            try
            {
                CacheNode changedNode = newNodeObj as CacheNode;

                // Save edits to the config file so the node persists if we restart master
                CacheConfig.Save(this.configFilePath, this.config);

                // Iterate through data nodes and send them all the new config
                var nodeEndPoints = this.config.Ring.GetNodeEndPoints();

                // Create a list of end points that need the configuration
                var endPointsToPush = new Dictionary<string, IPEndPoint>();

                if (IsThisNodeEndPoint(changedNode.IPEndPoint))
                {
                    //
                    //  The node changed is the master. Update it, but dont push.
                    //
                    this.localDataNode = changedNode;
                }

                foreach (var kvp in nodeEndPoints)
                {
                    // Skip self if master is also a data node
                    if (IsThisNodeEndPoint(kvp.Value))
                        continue;

                    endPointsToPush.Add(kvp.Key, kvp.Value);
                }

                PushConfigToNodes(endPointsToPush);
            }
            catch (Exception ex)
            {
                // We're on a background thread here so there's nothing we can do 
                // to let the caller know, since we returned a response already.
                CacheHelper.LogError("Unable to change node", ex);
            }
        }

        /// <summary>
        /// A request for node statistics
        /// </summary>
        public CacheMessage GetStats(byte[] data)
        {
            CacheMessage response = new CacheMessage(CacheResponseTypes.Accepted);

            UpdateRAMStats();

            // Binary Response Format
            //
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

            MemoryStream ms = new MemoryStream();

            using (BinaryWriter w = new BinaryWriter(ms))
            {
                w.Write(ToNetwork(this.dataByKey.Count));
                w.Write(ToNetwork(this.totalDataBytes));
                w.Write(ToNetwork(this.latestRAMBytes));

                string ramm = string.Format("{0:0.00}", this.ramMultiplier);
                byte[] rammb = GetBytes(ramm);
                w.Write(ToNetwork(rammb.Length));
                w.Write(rammb);

                long maxNumBytes =
                    this.localDataNode == null ? (long)0 : this.localDataNode.MaxNumBytes;
                w.Write(ToNetwork(maxNumBytes));

                byte status =
                    this.localDataNode == null ? (byte)0 : (byte)this.localDataNode.Status;
                w.Write(status);

                w.Flush();
                response.Data = ms.ToArray();
            }

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

                try
                {
                    CacheHelper.PerfCounters[CacheHelper.GetsPerSecond].Increment();
                }
                catch { }

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
                //     Status          byte
                //     ParseLocations  boolean
                //     if (ParseLocations)
                //          NumLocations    int
                //          [Locations]     ints
                // ]

                w.Write(ToNetwork(ring.Nodes.Count));
                foreach (var node in ring.Nodes.Values)
                {
                    byte[] hostNameData = GetBytes(node.HostName);
                    w.Write(ToNetwork(hostNameData.Length));
                    w.Write(hostNameData);
                    w.Write(ToNetwork(node.PortNumber));
                    w.Write(ToNetwork(node.MaxNumBytes));
                    w.Write((byte)node.Status);
                    w.Write(Convert.ToByte(includeLocations));

                    if (includeLocations)
                    {
                        w.Write(ToNetwork(node.Locations.Count));
                        foreach (var location in node.Locations)
                        {
                            w.Write(ToNetwork(location));
                        }
                    }
                }
                w.Flush();
                return ms.ToArray();
            }
        }

        Thread rebalanceThread;

        /// <summary>
        /// Starts the rebalancer
        /// </summary>
        private void StartRebalance()
        {
            // This is something we want to happen on a background thread
            rebalanceThread = new Thread(Rebalance);
            rebalanceThread.Start();

            CacheHelper.LogInfo("Started Rebalance Thread");
        }

        private void StopRebalance()
        {
            if (rebalanceThread != null && rebalanceThread.IsAlive)
            {
                try
                {
                    rebalanceThread.Abort();
                    CacheHelper.LogInfo("Aborted Rebalance Thread");
                }
                catch (ThreadAbortException)
                {
                    CacheHelper.LogError("Exception Aborting Rebalance Thread");
                }
            }                
        }

        /// <summary>
        /// Moves all keys that don't belong here any more to the node where
        /// they actually belong, according to current configuration
        /// </summary>
        private void Rebalance()
        {
            try
            {
                // The trick is to do this quickly, but do it without slowing down puts and gets

                // Get a list of all keys held by this node
                List<string> keys = new List<string>();

                this.dataLock.EnterReadLock();

                try
                {
                    foreach (var key in this.dataByKey.Keys)
                    {
                        keys.Add(key);
                    }
                }
                finally
                {
                    this.dataLock.ExitReadLock();
                }

                foreach (var key in keys)
                {
                    int hash = CacheHelper.GetConsistentHashCode(key);
                    CacheNode owningNode = this.config.Ring.GetNodeForHash(hash);
                    if (!IsThisNode(owningNode))
                    {
                        // We found an object that this node doesn't own any more
                        byte[] dataToMigrate = null;
                        try
                        {
                            this.dataLock.EnterWriteLock();

                            byte[] data;
                            if (this.dataByKey.TryGetValue(key, out data))
                            {
                                dataToMigrate = data;

                                // Delete the object
                                DeleteObjectInWriteLock(key);
                            }
                            else
                            {
                                // Not sure why it would have been deleted in the time
                                // since we got the list of keys.
                                CacheHelper.LogTrace(
                                    "Tried to migrate {0} but the key was missing", key);
                            }
                        }
                        finally
                        {
                            this.dataLock.ExitWriteLock();
                        }

                        // Create a request with the object to be migrated
                        CacheMessage request = new CacheMessage(CacheRequestTypes.PutObject);
                        using (MemoryStream ms = new MemoryStream())
                        {
                            BinaryWriter w = new BinaryWriter(ms);
                            w.Write(IPAddress.HostToNetworkOrder(key.Length));
                            w.Write(Encoding.UTF8.GetBytes(key));
                            w.Write(IPAddress.HostToNetworkOrder(dataToMigrate.Length));
                            w.Write(dataToMigrate);
                            w.Flush();
                            ms.Flush();
                            request.Data = ms.ToArray();
                        }

                        // Send the object to the node that owns it
                        var response = CacheHelper.SendRequest(request, owningNode.IPEndPoint);

                        CacheResponseTypes responseType = (CacheResponseTypes)response.MessageType;
                        if (responseType != CacheResponseTypes.ObjectOk)
                        {
                            CacheHelper.LogWarning(string.Format(
                                "Tried to migrate {0} to {1}, got response {2}",
                                key, owningNode.GetName(), responseType));
                        }
                    }
                }
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
                {
                    Task.Run(() =>
                    {
                        var kvp = new KeyValuePair<string, byte[]>(keyString, objectData);
                        this.PutObject(kvp);
                    });

                    try
                    {
                        CacheHelper.PerfCounters[CacheHelper.PutsPerSecond].Increment();
                    }
                    catch { }                    
                    
                    CacheMessage response = new CacheMessage(CacheResponseTypes.ObjectOk);
                    return response;
                }
                else
                {
                    return CreateReConfigureMessage();
                }
            }
        }

        //private void puts_CollectionChanged(object sender, System.Collections.Specialized.NotifyCollectionChangedEventArgs e)
        //{
        //    Task.Run(() =>
        //    {
        //        KeyValuePair<string, byte[]> kvp;
        //        foreach (var item in e.NewItems)
        //        {
        //            kvp = (KeyValuePair<string, byte[]>)item;
        //            this.PutObject(kvp);
        //            //this.puts.Remove(kvp);
        //        }
        //    });
        //}

        //private void PutObjectWorker()
        //{
        //    while (puts.Count > 0)
        //    {
        //        this.PutObject(puts[0]);
        //        this.puts.RemoveAt(0);
        //        Thread.Sleep(1);
        //    }

        //    Thread.Sleep(1000);
        //    this.PutObjectWorker();
        //}

        private void PutObject(KeyValuePair<string, byte[]> kvp)
        {
            string key = kvp.Key;
            byte[] data = kvp.Value;

            try
            {
                // We have to lock everything down to add something to the cache
                this.dataLock.EnterWriteLock();

                // Check how much memory we are using and eject old objects
                // from memory if necessary, before storing this object.
                // We can't just count the total number of bytes stored, unless we 
                // want people to take allocator inefficiency into account when they
                // configure the server.  Asking for how much memory the process is 
                // using right now would slow things down a lot.
               
                // We store the total number of bytes in the objects we're storing, but this is 
                // far short of the actual amount of RAM used by the process.  But we don't want
                // to check the actual process memory every time we put, since this will slow
                // down puts too much.  Check actual memory usage on a background thread, and 
                // store a multiplier for the relationship between the number of bytes stored 
                // and the number of bytes of RAM.

                // e.g. we're storing 1000 bytes, and using 2000 bytes of RAM, the multiplier is 2.

                // Use the multiplier times the number of data bytes stored as an approximation.

                long currentRawBytes = this.totalDataBytes;
                long approximateActualRAM = (long)(currentRawBytes * this.ramMultiplier);
                long approximateObjectRAM = (long)(data.Length * this.ramMultiplier);

                long initialCount = this.dataByKey.Count;
                long numObjectsDeleted = 0;

                while (approximateActualRAM + approximateObjectRAM > this.localDataNode.MaxNumBytes
                    && this.keysByTime.Count > 0)
                {
                    // Remove the oldest object from memory
                    var oldestKeys = this.keysByTime.Values[0];
                    if (oldestKeys.Count == 0) continue;
                    DeleteObjectInWriteLock(oldestKeys[0]);

                    numObjectsDeleted++;

                    // Re-caclulate RAM usage
                    currentRawBytes = this.totalDataBytes;
                    approximateActualRAM = (long)(currentRawBytes * this.ramMultiplier);
                }

                if (numObjectsDeleted > 0)
                {
                    // TODO - Once RAM is full, this will happen every time so we will
                    // want to remove this trace statement.
                    CacheHelper.LogTrace("Deleted {0} objects.  Initial count {1}, now {2}",
                        numObjectsDeleted, initialCount, this.dataByKey.Count);
                }
                else
                {
                    CacheHelper.LogTrace("Did not delete any objects to make room");
                }

                // Add or update the object
                if (this.dataByKey.ContainsKey(key))
                {
                    // Subtract the length of the old version of the object
                    this.totalDataBytes -= this.dataByKey[key].Length;

                    // Store the updated object
                    this.dataByKey[key] = data;
                }
                else
                {
                    // Store the new object
                    this.dataByKey.Add(key, data);
                }

                // Add the length of the object
                this.totalDataBytes += data.Length;

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
            }
            catch (Exception ex)
            {
                CacheHelper.LogError(ex.ToString());
            }
            finally
            {
                this.dataLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Returns true if this node is listening on the specified end point
        /// </summary>
        private bool IsThisNodeEndPoint(IPEndPoint endPoint)
        {
            return this.ipep.Equals(endPoint);
        }

        /// <summary>
        /// Returns true if this is the node specified.
        /// </summary>
        private bool IsThisNode(CacheNode node)
        {
            if (node.HostName.Equals(this.config.ListenerHostName) && 
                node.PortNumber.Equals(this.config.ListenerPortNumber))
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Returns true if this node owns the specified key.
        /// </summary>
        /// <remarks>
        /// This method tries once, and if it doesn't own the key, 
        /// it goes to the master for updated configuration, just in case the 
        /// cluster has changed.  If the cluster has changed, a rebalance is 
        /// started in the background.
        /// </remarks>
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

        /// <summary>
        /// Read a string from the reader
        /// </summary>
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
                throw new CacheMessageException(CacheResponseTypes.NotDataNode);

            // Binary request format:
            //
            // KeyLen    int (The length of the packet, which we already got)
            // Key       byte[] UTF8 string

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

            // If this node owns the key, remove the object.
            // If not, tell the client to reconfigure.
            if (IsThisNode(keyString))
            {
                Task.Run(() =>
                {
                    DeleteObject(keyString);
                });

                return new CacheMessage(CacheResponseTypes.ObjectOk);
            }
            else
            {
                return CreateReConfigureMessage();
            }

        }

        /// <summary>
        /// Called internally by methods that have already entered a write lock.
        /// </summary>
        /// <param name="keyString"></param>
        /// <returns></returns>
        private bool DeleteObjectInWriteLock(string keyString)
        {
            bool inDataByKey = false;

            if (this.dataByKey.ContainsKey(keyString))
            {
                inDataByKey = true;

                int size = this.dataByKey[keyString].Length;

                // Remove the object from the main list
                this.dataByKey.Remove(keyString);

                // Update the total number of raw bytes
                this.totalDataBytes -= size;
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

                        // If we removed the only entry in the bucket, remove the bucket
                        if (keys.Count == 0)
                        {
                            this.keysByTime.Remove(keyPutTime);
                        }
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

            return inDataByKey;
        }

        private void DeleteObject(string keyString)
        {
            // This method is called after we have determined that this node owns the key
            try
            {
                this.dataLock.EnterWriteLock();

                DeleteObjectInWriteLock(keyString);

                //CacheMessage response = new CacheMessage();

                //if (inDataByKey)
                //    response.MessageType = 
                //        (byte)CacheResponseTypes.ObjectOk;
                //else
                //    response.MessageType = 
                //        (byte)CacheResponseTypes.ObjectMissing;

                try
                {
                    CacheHelper.PerfCounters[CacheHelper.DeletesPerSecond].Increment();
                }
                catch { }

                //return response;
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
            response.MessageType = (byte)CacheResponseTypes.Accepted;

            this.config.Ring = DeserializeNodes(data);

            CacheHelper.LogTrace("Got new config from master: \r\n{0}", 
                this.config.Ring.GetTrace());

            bool found = false;
            foreach (var node in this.config.Ring.Nodes.Values)
            {
                if (IsThisNode(node))
                {
                    this.localDataNode = node;
                    //
                    //  If the node is in the ring its status should be up.
                    //
                    this.localDataNode.Status = CacheNodeStatus.Up;

                    if (this.localDataNode.Status == CacheNodeStatus.Migrating)
                    {
                        //
                        //  If were in the middle of rebalance we need to stop!
                        //
                        this.StopRebalance();
                    }
                    
                    found = true;
                }
            }

            if (!found)
            {
                // This node was taken down!
                // TODO - The master should probably tell this data node explicitly so
                // that we know for sure, and we can start rebalancing immediately.
                this.localDataNode.Status = CacheNodeStatus.Migrating;
                StartRebalance();
            }

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

                return new CacheMessage(CacheResponseTypes.UnknownNode);
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
        async Task WriteResponse(NetworkStream n, byte responseType, byte[] responseData)
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

            if (dataLength > (1024 * 1024))
            {
                throw new Exception("Exceeded max data length");
            }

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
                throw new Exception("data shorter than expected");
            }
            node.HostName = Encoding.UTF8.GetString(hostData);
            node.PortNumber = FromNetwork(reader.ReadInt32());
            node.MaxNumBytes = FromNetwork(reader.ReadInt64());
            node.Status = (CacheNodeStatus)reader.ReadByte();
            bool parseLocations = reader.ReadBoolean();

            if (parseLocations)
            {
                int numLocations = FromNetwork(reader.ReadInt32());
                if (numLocations > 0)
                {
                    for (int i = 0; i < numLocations; i++)
                    {
                        int location = FromNetwork(reader.ReadInt32());
                        node.Locations.Add(location);
                    }
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

            bool hasLocations = false;

            // Deserialize the config
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(ms))
                {
                    // NumNodes            int
                    // [
                    //     HostLen         int
                    //     Host            byte[] UTF8 string
                    //     Port            int
                    //     MaxNumBytes     long
                    //     Status          byte
                    //     ParseLocations  boolean
                    //     if (ParseLocations)
                    //          NumLocations    int
                    //          [Locations]     ints
                    // ]

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

                CacheHelper.LogTrace("RegisterWithMaster not necessary since this is the master");
                
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
                CacheMessage response = CacheHelper.SendRequest(request, this.config.MasterIPEndPoint);
            
                if (response.MessageType == (byte)CacheResponseTypes.Configuration)
                {
                    CacheRing ring = DeserializeNodes(response.Data);

                    CacheHelper.LogTrace("Got the ring configuration from master");

                    return ring;
                }

                // We'll get an unknown node response during the time
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

        private void UpdateRAMStats()
        {
            var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
            this.latestRAMBytes = currentProcess.WorkingSet64;

            // Figure out a multiplier to apply to the number of actual 
            // data bytes stored to get close to real RAM usage.

            long total = this.totalDataBytes;
            long latest = this.latestRAMBytes;

            if (latest > 0 && total > 0)
            {
                // e.g. Latest is 1024 and total is 512, multiplier is 2.0
                this.ramMultiplier = (double)((double)latest / (double)total);
            }
            else
            {
                this.ramMultiplier = 1.5;
            }

            if (this.ramMultiplier < 1) this.ramMultiplier = 1;
            if (this.ramMultiplier > 3) this.ramMultiplier = 3;
        }

        /// <summary>
        /// Check the RAM usage of the process every few seconds
        /// </summary>
        private void MonitorRAM()
        {
            while (this.shouldRun)
            {
                try
                {
                    UpdateRAMStats();

                    StoppablePause(5);
                }
                catch (Exception ex)
                {
                    CacheHelper.LogTrace("Failed to monitor RAM: {0}", ex.ToString());
                }
            }
        }

        private int ToNetwork(int i)
        {
            return IPAddress.HostToNetworkOrder(i);
        }

        private long ToNetwork(long i)
        {
            return IPAddress.HostToNetworkOrder(i);
        }

        private int FromNetwork(int i)
        {
            return IPAddress.NetworkToHostOrder(i);
        }

        private long FromNetwork(long i)
        {
            return IPAddress.NetworkToHostOrder(i);
        }

        private byte[] GetBytes(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        private string GetString(byte[] b)
        {
            return Encoding.UTF8.GetString(b);
        }
    }
}