using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;

namespace LoopCacheLib
{
    /// <summary>A consistent hash ring</summary>
    /// <remarks>This class stores nodes and virtual node locations on a 32 bit ring, 
    /// which provides a consistent hash location for objects.  When the node
    /// configuration changes, relatively few objects will need to be remapped.</remarks>
    public class CacheRing
    {
        /// <summary>The nodes in this ring</summary>
        public SortedList<string, CacheNode> Nodes { get; private set; }

        /// <summary>A copy of the virtual node locations for fast lookup</summary>
        public SortedList<int, CacheNode> SortedLocations { get; private set; }

        /// <summary>Used to synchronize access to the ring</summary>
        private ReaderWriterLockSlim ringLock = new ReaderWriterLockSlim();

        /// <summary>Construct a new ring</summary>
        public CacheRing()
        {
            this.Nodes = new SortedList<string, CacheNode>();
            this.SortedLocations = new SortedList<int, CacheNode>();
        }

        /// <summary>Get a string that dumps the state of this ring</summary>
        public String GetTrace()
        {
            StringBuilder sb = new StringBuilder();

            if (this.Nodes == null)
            {
                sb.Append("Nodes null\r\n");
            }
            else if (this.Nodes.Count == 0)
            {
                sb.Append("Nodex empty\r\n");
            }
            else
            {
                foreach (var node in this.Nodes.Values)
                {
                    sb.Append(node.GetTrace());
                    sb.Append("\r\n");
                }
            }

            return sb.ToString();
        }

        /// <summary>Get the node that owns the specified hash code</summary>
        public CacheNode GetNodeForHash(int hashCode)
        {
            this.ringLock.EnterReadLock();
            
            try
            {
                if (this.SortedLocations == null || this.SortedLocations.Count == 0) 
                {
                    throw new Exception("SortedLocations are null or empty");
                }

                // ---- n1 ----- n2 ----- n3 ----- n4 ----- n5
                // --------- h -------------------------------
                // 100  200 300  400 500  600 700  800 900  1000 

                // For now simply iterate over the list until we find the bucket.
                // On the ring, the node to clockwise owns the angle between it and 
                // the last node.  Above, n2 owns from 201 to 400, and therefore h.
                // We can surely speed this up by dividing and conquering.
                CacheNode firstNode = null;
                foreach (var kvp in this.SortedLocations)
                {
                    if (firstNode == null) firstNode = kvp.Value;
                    if (kvp.Key >= hashCode) return kvp.Value;
                }

                // We wrapped around Int.MAX
                return firstNode;
            }
            finally
            {
                this.ringLock.ExitReadLock();
            }
        }

        /// <summary>Scan the node list to find a node with the specified IP</summary>
        public CacheNode FindNodeByIP(IPEndPoint endPoint)
        {
            try
            {
                this.ringLock.EnterReadLock();
                foreach (var node in this.Nodes.Values)
                {
                    if (node.IPEndPoint == null)
                    {
                        // Why is this null?  It never should be.
                        CacheHelper.LogWarning(string.Format(
                            "FindNodeByIP Node {0} IPEndPoint is null",
                            node.GetName()));

                        node.IPEndPoint = 
                            CacheHelper.GetIPEndPoint(node.HostName, node.PortNumber);
                    }

                    if (node.IPEndPoint.Equals(endPoint))
                    {
                        return node;
                    }
                }
                return null;
            }
            finally
            {
                this.ringLock.ExitReadLock();
            }
        }

        /// <summary>Scan the node list to find a node with the specified name</summary>
        public CacheNode FindNodeByName(string name)
        {
            try
            {
                this.ringLock.EnterReadLock();
                foreach (var node in this.Nodes.Values)
                {
                    if (node.GetName().Equals(name))
                    {
                        return node;
                    }
                }
                return null;
            }
            finally
            {
                this.ringLock.ExitReadLock();
            }
        }

        /// <summary>Adds a node to the ring</summary>
        /// <remarks>This is only called on the master.  Data nodes will
        /// simply reload everything when something changes</remarks>
        public void AddNode(CacheNode node)
        {
            this.ringLock.EnterWriteLock();
            try
            {
                // Make sure this node isn't already in the ring
                string nodeName = node.GetName();
                if (this.Nodes.ContainsKey(nodeName))
                {
                    throw new Exception("Already added node " + nodeName);
                }
                node.Status = CacheNodeStatus.Down;

                this.Nodes.Add(nodeName, node);

                DetermineNodeLocations();
                LookupEndPoints();
            }
            finally
            {
                this.ringLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Remove the node from the ring.
        /// </summary>
        /// <remarks>Figures out new node locations before returning.</remarks>
        /// <param name="name"></param>
        public void RemoveNodeByName(string name)
        {
            this.ringLock.EnterWriteLock();
            try
            {
                this.Nodes.Remove(name);

                DetermineNodeLocations();
                LookupEndPoints();
            }
            finally
            {
                this.ringLock.ExitWriteLock();
            }
        }

        /// <summary>Data nodes call this method to populate the nodes with 
        /// location and do DNS lookups, since they get a simplified node list
        /// from the master when the master notifies them of ring changes</summary>
        public void PopulateNodes()
        {
            // We enter a read lock here.  We aren't adding or removing nodes, 
            // just setting properties on them.

            this.ringLock.EnterReadLock();
            try
            {
                DetermineNodeLocations();
                LookupEndPoints();
            }
            finally
            {
                this.ringLock.ExitReadLock();
            }
        }

        /// <summary>Look up the IPs for the nodes and store them</summary>
        /// <remarks>Caller should enter a read lock</remarks>
        private void LookupEndPoints()
        {
            foreach (var node in this.Nodes.Values)
            {
                node.IPEndPoint = CacheHelper.GetIPEndPoint(node.HostName, node.PortNumber);
            }
        }

        /// <summary>
        /// Populate nodes with their locations.
        /// </summary>
        /// <remarks>Calling this multiple times should result in the same 
        /// locations.  Adding or removing a node, or changing a node's size, 
        /// should have a minimal effect on the node locations.  This method
        /// must be called after making any changes to the nodes.  The caller
        /// should enter a write lock.</remarks>
        private void DetermineNodeLocations()
        {
            // Reset the locations
            this.SortedLocations = new SortedList<int, CacheNode>();
            
            // Figure out the total available memory in the cluster
            long totalMemory = 0;
            foreach (var node in this.Nodes.Values)
            {
                // Clear out all previous node locations
                node.ResetLocations();

                totalMemory += node.MaxNumBytes;
            }

            // We'll create an average of 100 virtual locations per node
            int totalVirtualNodes = 100 * this.Nodes.Count;

            foreach (var node in this.Nodes.Values)
            {
                // Figure out the percentage of memory that this node has
                double percentage = (double)node.MaxNumBytes/(double)totalMemory;

                // Give this node an appropriate number of locations
                int numLocations = (int)Math.Round(totalVirtualNodes * percentage);

                // Get a hash code for each location and save it
                for (int i = 0; i < numLocations; i++)
                {
                    int hashCode = CacheHelper
                        .GetConsistentHashCode(node.GetVirtualNodeName(i));

                    int sanityCheck = 0;
                    int maxSanity = 100;

                    // Handle collisions - we can't let two nodes share the same spot.
                    while (this.SortedLocations.ContainsKey(hashCode) && 
                            sanityCheck++ < maxSanity)
                    {
                        // Just increment by 1.  In the rare case of a collision, 
                        // this will be a very small bucket.
                        hashCode++;
                    }

                    if (sanityCheck >= maxSanity)
                    {
                        throw new Exception("Tried " + maxSanity.ToString() + 
                                " times to avoid a collision");
                    }

                    // Add the location to the ring's master list
                    this.SortedLocations.Add(hashCode, node);

                    // Add the location to the node
                    node.Locations.Add(hashCode);
                }
            }
        }

        /// <summary>Get a list of node end points</summary>
        /// <remarks>The master node uses this to send messages to the data nodes</remarks>
        /// <returns>A list of NodeName=>IPEndPoint</returns>
        public SortedList<string, IPEndPoint> GetNodeEndPoints()
        {
            SortedList<string, IPEndPoint> list = new SortedList<string, IPEndPoint>();

            this.ringLock.EnterReadLock();

            try
            {
                foreach (var node in this.Nodes)
                {
                    list.Add(node.Key, node.Value.IPEndPoint);
                }
            }
            finally
            {
                this.ringLock.ExitReadLock();
            }

            return list;
        }

        /// <summary>Set the status of a node</summary>
        public void SetNodeStatus(string nodeName, CacheNodeStatus status)
        {
            SetNodeStatus(FindNodeByName(nodeName), status);
            
        }

        /// <summary>Set the status of a node</summary>
        public void SetNodeStatus(IPEndPoint endPoint, CacheNodeStatus status)
        {
            SetNodeStatus(FindNodeByIP(endPoint), status);
        }

        /// <summary>Set the status of a node</summary>
        public void SetNodeStatus(CacheNode node, CacheNodeStatus status)
        {
            if (node == null) return;

            // I don't think it's necessary to enter a read lock here.

            node.Status = status;

            // TODO - What if status is Migrating?  It shouldn't be part of the ring.
        }

        /// <summary>Test basic ring functionality.</summary>
        /// <remarks>Throws an exception or returns false on failure</remarks>
        public static bool Test()
        {
            CacheRing ring = new CacheRing();

			CacheNode nodeA = new CacheNode();
			nodeA.HostName = "localhost";
			nodeA.PortNumber = 1;
			nodeA.MaxNumBytes = CacheConfig.ParseMaxNumBytes("48Mb");
			ring.Nodes.Add(nodeA.GetName(), nodeA);

			CacheNode nodeB = new CacheNode();
			nodeB.HostName = "localhost";
			nodeB.PortNumber = 2;
			nodeB.MaxNumBytes = CacheConfig.ParseMaxNumBytes("12Mb");
			ring.Nodes.Add(nodeB.GetName(), nodeB);

			CacheNode nodeC = new CacheNode();
			nodeC.HostName = "localhost";
			nodeC.PortNumber = 3;
			nodeC.MaxNumBytes = CacheConfig.ParseMaxNumBytes("64Mb");
			ring.Nodes.Add(nodeC.GetName(), nodeC);

			// Hard-code some locations so we can make sure objects get assigned
			// to the correct virtual node.

			ring.SortedLocations.Add(10, nodeA);
			nodeA.Locations.Add(10);
			ring.SortedLocations.Add(-10, nodeB);
			nodeB.Locations.Add(-10);
			ring.SortedLocations.Add(20, nodeC);
			nodeC.Locations.Add(20);
			ring.SortedLocations.Add(50, nodeA);
			nodeA.Locations.Add(50);
			ring.SortedLocations.Add(60, nodeC);
			nodeC.Locations.Add(60);

			var node = ring.GetNodeForHash(5);
			if (node != nodeA)
			{
				throw new Exception(string.Format
                        ("Hash 5 should belong to nodeA, not {0}", node.GetName()));
			}
			
			node = ring.GetNodeForHash(int.MaxValue);
			if (node != nodeB)
			{
				throw new Exception(string.Format
				    ("Hash Integer.MAX should belong to nodeB, not {0}", node.GetName()));
			}
			
			node = ring.GetNodeForHash(20);
			if (node != nodeC)
			{
				throw new Exception(string.Format
				    ("Hash 20 should belong to nodeC, not {0}", node.GetName()));
			}
			
			node = ring.GetNodeForHash(25);
			if (node != nodeA)
			{
				throw new Exception(string.Format
				    ("Hash 25 should belong to nodeA, not {0}", node.GetName()));
			}

			// Now get rid of those hard coded locations and let the algorithm decide
			ring.DetermineNodeLocations();

			// Make sure the master list and the nodes agree
			foreach (var n in ring.Nodes.Values)
			{
				foreach (var location in n.Locations)
				{
					if (!ring.SortedLocations.ContainsKey(location) || 
							ring.SortedLocations[location] != n)
					{
				        throw new Exception(string.Format
						    ("Location {0} in node {1} not found", location, node.GetName()));
					}
				}
			}

			foreach (var loc in ring.SortedLocations)
			{
				string nodeName = loc.Value.GetName();
				if (!ring.Nodes.ContainsKey(nodeName))
				{
				    throw new Exception(string.Format
					    ("ring.Nodes missing {0}", nodeName));
				}
				if (!loc.Value.Locations.Contains(loc.Key))
				{
                    throw new Exception(string.Format
					    ("node {0} does not have {1}", nodeName, loc.Key));
				}
			}

			//Console.WriteLine(ring.GetTrace());

			// Now let's place a bunch of values and see how many of them change
			// when we make changes to the node configuration.

			//Console.WriteLine("About to place objects");

			// nodeName => List of hashes
			Dictionary<string, List<int>> map = new Dictionary<string, List<int>>();
			
			for (int i = 0; i < 100000; i++)
			{
				Guid g = Guid.NewGuid();
				int hash = CacheHelper.GetConsistentHashCode(g.ToString());
				CacheNode n = ring.GetNodeForHash(hash);
				string nodeName = n.GetName();
				List<int> hashes;
				if (!map.TryGetValue(nodeName, out hashes))
				{
					hashes = new List<int>();
					map[nodeName] = hashes;
				}
				hashes.Add(hash);
			}

			//foreach (var nodeName in map.Keys)
			//{
			//	Console.WriteLine("{0} has {1} hashes", 
			//			nodeName, map[nodeName].Count);
			//}

			//Console.WriteLine("Modifying sizes and replacing objects");
			nodeC.MaxNumBytes = CacheConfig.ParseMaxNumBytes("48Mb"); // was 64Mb
			ring.PopulateNodes();
			int numChanged = 0;
            int numTotal = 0;

			foreach (var nodeName in map.Keys)
			{
				foreach (int hash in map[nodeName])
				{
                    numTotal++;
					CacheNode n = ring.GetNodeForHash(hash);
					if (!(nodeName.Equals(n.GetName())))
					{
						numChanged++;
					}
				}
			}

            if (numChanged >= numTotal)
                throw new Exception("Number of changed hases >= total number of hashes");

            // TODO - Caclulate an acceptable percentage

			//Console.WriteLine("{0} hashes changed which node they were assigned to", 
			//		numChanged);

			return true;
    
        }
    }

    /// <summary>Represents the status of a node</summary>
    public enum CacheNodeStatus
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

    /// <summary>A single real node in the ring</summary>
    /// <remarks>A node contains many virtual node locations</remarks>
    public class CacheNode
    {
        /// <summary>The name of the host where this node is listening</summary>
        public string HostName { get; set; }

        /// <summary>The TCP port this node is bound to</summary>
        public int PortNumber { get; set; }

        /// <summary>The node's endpoint.  This should be set explicilty once by the code
        /// that creates the instance, since creating an endpoint requires a DNS lookup
        /// </summary>
        public IPEndPoint IPEndPoint { get; set; }

        /// <summary>The maximum number of bytes this node will store before ejecting
        /// older objects from memory</summary>
        public long MaxNumBytes { get; set; }

        /// <summary>The current status of this node</summary>
        public CacheNodeStatus Status { get; set; }

        /// <summary>The virtual hash ring locations owned by this node</summary>
        public List<int> Locations { get; private set;}

        /// <summary>Construct a new instance of a node</summary>
        public CacheNode()
        {
            this.Locations = new List<int>();
        }

        /// <summary>Get the unique name for this node.  HOSTNAME:PortNumber</summary>
        /// <remarks>HostName gets converted to upper case</remarks>
        public string GetName()
        {
            return CreateName(this.HostName, this.PortNumber);
        }

        /// <summary>Get the unique name for a node.</summary>
        /// <remarks>HostName gets converted to upper case</remarks>
        public static string CreateName(string hostName, int portNumber)
        {
            return (hostName + ":" + portNumber.ToString()).ToUpper();
        }

        /// <summary>Get a string dump of the state of this node</summary>
        public string GetTrace()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendFormat("\t{0}:{1} {2:0,0}b {3}\r\n", 
                    this.HostName, this.PortNumber, this.MaxNumBytes, this.Status.ToString());

            foreach (var location in this.Locations)
            {
                sb.AppendFormat("\t\t{0}\r\n", location);
            }

            return sb.ToString();
        }

        /// <summary>Get the concatenated name of the virtual node</summary>
        /// <remarks>This is used to calculate the hash ring location</remarks>
        public string GetVirtualNodeName(int virtualNodeIndex)
        {
            return string.Format("{0}_{1}_{2}", 
                   this.HostName, 
                   this.PortNumber, 
                   virtualNodeIndex);
        }

        /// <summary>Removes the current node locations and replaces them
        /// with an empty list.</summary>
        public void ResetLocations()
        {
            this.Locations = new List<int>();
        }

        /// <summary>This override produces a string that can be saved to the config file
        /// as a Node line</summary>
        public override string ToString()
        {
            return string.Format("Node\t{0}:{1}\t{2}",
                this.HostName, this.PortNumber, this.MaxNumBytes);
        }
    }
}
