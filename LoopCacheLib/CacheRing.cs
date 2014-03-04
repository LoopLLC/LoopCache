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

        /// <summary>
        /// Populate nodes with their locations.
        /// </summary>
        /// <remarks>Calling this multiple times should result in the same 
        /// locations.  Adding or removing a node, or changing a node's size, 
        /// should have a minimal effect on the node locations.  This method
        /// must be called after making any changes to the nodes.</remarks>
        public void DetermineNodeLocations()
        {
            this.ringLock.EnterWriteLock();

            try
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
            finally
            {
                this.ringLock.ExitWriteLock();
            }
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
        Questionable = 3, 

		/// <summary>
		/// The node is being shut down gracefully and is migrating its objects.
		/// </summary>
		/// <remarks>
		/// It no longer owns its objects and is not part of the cache ring.
		/// </remarks>
		Migrating
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
            return (this.HostName + ":" + this.PortNumber.ToString()).ToUpper();
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
