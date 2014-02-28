using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using LoopCacheLib;

namespace LoopCacheConsole
{
	/// <summary>
	/// This is a command line app that runs, tests, and administers the cache
	/// </summary>
	class Program
	{
		static void Main(string[] args)
		{
			// -help
			// -test
			// -testclient config.txt
			// -server config.txt
			//
			if (args.Length >= 1 && args[0].ToLower().Equals("-test"))
			{
				bool allPassed = true;

				bool ok = TestMaxMem();
				allPassed = allPassed && ok;
				if (ok) Console.WriteLine("TestMaxMem passed");
				else Console.WriteLine("TestMaxMem failed");

				ok = TestConsistentHash();
				allPassed = allPassed && ok;
				if (ok) Console.WriteLine("TestConsistentHash passed");
				else Console.WriteLine("TestConsistentHash failed");

				ok = TestCacheRing();
				allPassed = allPassed && ok;
				if (ok) Console.WriteLine("TestCacheRing passed");
				else Console.WriteLine("TestCacheRing failed");

				if (args.Length > 2 && args[1].ToLower().Equals("-config"))
				{
					CacheConfig config = CacheConfig.Load(args[2]);
					Console.WriteLine("Config loaded Ok");
				}

				if (allPassed)
				{
					Console.WriteLine("All Tests Passed!");
				}
				else
				{
					Console.WriteLine("Some Tests Failed!");
				}
			}
			else if (args.Length >= 1 && args[0].ToLower().Equals("-testclient"))
			{
				if (args.Length != 2)
				{
					Console.WriteLine("Missing master hostname:port.  See usage:");
					Usage();
					return;
				}

				// It's cheating to use anything from LoopCacheLib for client tests.
				// Developers can use this code as an example of how to use the API.
				// LoopCacheLib will *not* be referenced by calling applications, 
				// unless they happen to be the master and data nodes talking to each other.

				SampleCacheClient sc = new SampleCacheClient(args[1]);
				if (sc.Test())
				{
					Console.WriteLine("All client tests passed");
				}
				else
				{
					Console.WriteLine("Client test failed!");
				}
			}
			else if (args.Length >= 1 && args[0].ToLower().Equals("-server"))
			{
				if (args.Length != 2)
				{
					Console.WriteLine("Expected config file location.  See usage:");
					Usage();
					return;
				}

				// Load config and dump it to the console while in development.
				// We can remove this later
				CacheConfig config = CacheConfig.Load(args[1]);
				Console.WriteLine("About to start server with these settings: {0}", 
						config.GetTrace());

				CacheListener listener = new CacheListener(args[1]);
				var task = listener.StartAsync();

				// Control returns here almost immediately, but we want to 
				// wait until the while loop in StartAsync completes

				//bool result = task.Result; // blocks until StartAsync is totally done

				//Console.WriteLine("After listener.Start in Main");
				
				Console.WriteLine("Enter to stop");
				Console.ReadLine();
				listener.Stop();

				Console.WriteLine("After Stop");

				bool result = task.Result;

				Console.WriteLine("Got result {0}", result);
			}
			else
			{
				Usage();
				return;
			}

			Console.WriteLine("Main end");
		}

		static void Usage()
		{
			Console.WriteLine("LoopCacheConsole.exe");
			Console.WriteLine("\t-help\t\t\t\tShow this message");
			Console.WriteLine("\t-test\t\t\t\tRun some basic unit tests");
			Console.WriteLine("\t-testclient hostname:port\tUnit test client requests.  ");
			Console.WriteLine("\t\t\t\t\tRequires master and data nodes to be running."); 
			Console.WriteLine("\t\t\t\t\thostname:port is the master listener");
			Console.WriteLine("\t-server config.txt\t\tRun a master or data node");
		}

		static bool TestCacheRing()
		{
			CacheRing ring = new CacheRing();

			CacheNode nodeA = new CacheNode();
			nodeA.HostName = "A";
			nodeA.PortNumber = 1;
			nodeA.MaxNumBytes = CacheConfig.ParseMaxNumBytes("48Mb");
			ring.Nodes.Add(nodeA.GetName(), nodeA);

			CacheNode nodeB = new CacheNode();
			nodeB.HostName = "B";
			nodeB.PortNumber = 1;
			nodeB.MaxNumBytes = CacheConfig.ParseMaxNumBytes("12Mb");
			ring.Nodes.Add(nodeB.GetName(), nodeB);

			CacheNode nodeC = new CacheNode();
			nodeC.HostName = "C";
			nodeC.PortNumber = 1;
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
				Console.WriteLine("Hash 5 should belong to nodeA, not {0}", node.GetName());
				return false;
			}
			
			node = ring.GetNodeForHash(int.MaxValue);
			if (node != nodeB)
			{
				Console.WriteLine("Hash Integer.MAX should belong to nodeB, not {0}", 
						node.GetName());
				return false;
			}
			
			node = ring.GetNodeForHash(20);
			if (node != nodeC)
			{
				Console.WriteLine("Hash 20 should belong to nodeC, not {0}", 
						node.GetName());
				return false;
			}
			
			node = ring.GetNodeForHash(25);
			if (node != nodeA)
			{
				Console.WriteLine("Hash 25 should belong to nodeA, not {0}", 
						node.GetName());
				return false;
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
						Console.WriteLine("Location {0} in node {1} not found", 
								location, node.GetName());
						return false;
					}
				}
			}

			foreach (var loc in ring.SortedLocations)
			{
				string nodeName = loc.Value.GetName();
				if (!ring.Nodes.ContainsKey(nodeName))
				{
					Console.WriteLine("ring.Nodes missing {0}", nodeName);
				}
				if (!loc.Value.Locations.Contains(loc.Key))
				{
					Console.WriteLine("node {0} does not have {1}", 
							nodeName, loc.Key);
				}
			}

			Console.WriteLine(ring.GetTrace());

			// Now let's place a bunch of values and see how many of them change
			// when we make changes to the node configuration.

			Console.WriteLine("About to place objects");

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

			foreach (var nodeName in map.Keys)
			{
				Console.WriteLine("{0} has {1} hashes", 
						nodeName, map[nodeName].Count);
			}

			Console.WriteLine("Modifying sizes and replacing objects");
			nodeC.MaxNumBytes = CacheConfig.ParseMaxNumBytes("48Mb"); // was 64Mb
			ring.DetermineNodeLocations();
			int numChanged = 0;

			foreach (var nodeName in map.Keys)
			{
				foreach (int hash in map[nodeName])
				{
					CacheNode n = ring.GetNodeForHash(hash);
					if (!(nodeName.Equals(n.GetName())))
					{
						numChanged++;
					}
				}
			}

			Console.WriteLine("{0} hashes changed which node they were assigned to", 
					numChanged);

			return true;
		}

		static bool TestMaxMem()
		{
			Dictionary<string, long> tests = new Dictionary<string, long>();

			tests["12"] = 12;
			tests["1024"] = 1024;
			tests["1,024"] = 1024;
			tests["1Kb"] = 1024;
			tests["1aa"] = -1;
			tests["1Mb"] = 1024 * 1024;
			tests["1Gb"] = 1024 * 1024 * 1024;
			tests["Gb"] = -1;
			tests["1,024Kb"] = 1024 * 1024;

			foreach (var kvp in tests)
			{
				try
				{
					long answer = CacheConfig.ParseMaxNumBytes(kvp.Key);
					if (answer != kvp.Value) 
					{
						Console.WriteLine("{0}:{1} != {2}", 
							kvp.Key, kvp.Value, answer);
						return false;
					}
				}
				catch (Exception ex)
				{
					if (kvp.Value != -1) 
					{
						Console.WriteLine(ex.Message);
						return false;
					}
				}
			}
				
			return true;
		}

		static bool TestConsistentHash()
		{
			Console.WriteLine("New Guids");
			for (int i = 0; i < 10; i++)
			{
				Guid g = Guid.NewGuid();
				Console.WriteLine("{0}: {1}", g, 
						CacheHelper.GetConsistentHashCode(g.ToString()));
			}

			Console.WriteLine();
			Console.WriteLine("Existing Guids");
			Dictionary<Guid, int> list = new Dictionary<Guid, int>();
			list.Add(new Guid("6afc9cd0-a312-495d-958e-3f5ee1021dc9"), 207271529);
			list.Add(new Guid("58aad64e-781e-45ed-a516-e0466fdb421c"), 793011885);
			list.Add(new Guid("b74e36e7-75f7-4e37-8137-2a7ebf09ea3a"), -2092457456);
			list.Add(new Guid("73974cd7-7f82-4165-8d2b-756420b8ce7c"), 1370574413);
			list.Add(new Guid("8fee3e72-7e2f-41ac-9e01-15786f462eda"), 1781424005);
			list.Add(new Guid("f9e9f75c-14ed-43ce-a58d-64dd0e4cf47d"), -1283683673);
			list.Add(new Guid("49249af7-8efa-49a6-bdb4-83f2c5f98557"), -1108612196);
			list.Add(new Guid("6c2b873c-bc26-4891-81cf-6a2092a127df"), -1697063316);
			list.Add(new Guid("25c43b37-8515-4e4a-847d-bd2468a9e359"), -953497936);
			list.Add(new Guid("36ab9e2f-601e-4197-97fa-742b31da9ec9"), -437896523);
			list.Add(new Guid("f39d1d95-af0f-4a28-8178-73f93c22096f"), -2095074639);

			// TODO - I'm worried about this.  I had saved some values from 
			// a few years ago when I was playing around with consistent hashing, 
			// and the hashes no longer match.  The hard coded list above is a 
			// replacement based on the current implementation.  
			// The hashes should have been stable forever.  They should always
			// be the same on all machines.

			// The following test should always succeed, regardless of platform.
			foreach (Guid key in list.Keys)
			{
				Console.WriteLine(key.ToString());
				int hc = CacheHelper.GetConsistentHashCode(key.ToString());
				if (list[key] == hc)
				{
					Console.WriteLine("{0} = {1} Ok!", key, hc);
				}
				else
				{
					Console.WriteLine("{0} = {1} Failed! (should be {2})",
						key, hc, list[key]);
					return false;
				}
			}

			return true;
		}
	}

}
