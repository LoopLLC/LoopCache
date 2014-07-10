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
            // -add 
            // -remove TODO
            // -list 
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
				// LoopCacheLib will *not* be referenced by client applications.

				CacheClient client = new CacheClient(args[1]);
				if (client.Test())
				{
					Console.WriteLine("All client tests passed");
				}
				else
				{
					Console.WriteLine("Client test failed!");
				}
            }
            else if (args.Length >= 1 && args[0].ToLower().Equals("-test3"))
            {
                if (args.Length != 2)
                {
                    Console.WriteLine("Missing master hostname:port.  See usage:");
                    Usage();
                    return;
                }

                // It's cheating to use anything from LoopCacheLib for client tests.
                // Developers can use this code as an example of how to use the API.
                // LoopCacheLib will *not* be referenced by client applications.

                CacheClient client = new CacheClient(args[1]);
                if (client.TestThreeNodes())
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

                CacheHelper.InitPerformanceCounters();

				CacheListener listener = new CacheListener(args[1]);
				var task = listener.StartAsync();
				
				Console.WriteLine("Press Enter to stop");
				Console.ReadLine();
				listener.Stop();

				Console.WriteLine("After Stop");

				bool result = task.Result;

				Console.WriteLine("Got result {0}", result);
			}
            else if (args.Length >= 1 && args[0].ToLower().Equals("-add"))
            {
                if (args.Length != 4)
                {
                    Usage();
                    return;
                }
                string masterHostPortStr = args[1];
                string newNodeHostPortStr = args[2];
                string maxNumBytesStr = args[3];

                CacheClient client = new CacheClient(masterHostPortStr);
                long maxNumBytes;
                if (!long.TryParse(maxNumBytesStr, out maxNumBytes))
                {
                    Console.WriteLine("New node maxNumBytes should be a long");
                    Usage();
                    return;
                }
                if (client.AddNode(newNodeHostPortStr, maxNumBytes))
                {
                    Console.WriteLine("New node added");
                }
                else
                {
                    Console.WriteLine("Unable to add new node, check master server logs");
                }
            }
            else if (args.Length >=1 && args[0].ToLower().Equals("-list"))
            {
                if (args.Length != 2)
                {
                    Console.WriteLine("Missing master hostname:port.  See usage:");
                    Usage();
                    return;
                }

                CacheClient client = new CacheClient(args[1]);
                client.PrintList();
            }
            else if (args.Length >= 1 && args[0].ToLower().Equals("-testservice"))
            {
                if (args.Length != 2)
                {
                    Console.WriteLine("Missing node hostname:port.  See usage:");
                    Usage();
                    return;
                }

                CacheClient client = new CacheClient(args[1]);

                // Make sure the node is listening
                if (!client.Ping())
                {
                    Console.WriteLine("Unable to ping master node");
                    return;
                }

                if (!client.GetConfig())
                {
                    Console.WriteLine("Unable to get ring configuration");
                    return;
                }

                bool allTestsPassed = true;

                List<Guid> testObjects = new List<Guid>();
                for (int i = 0; i < 1000; i++)
                {
                    Guid g = Guid.NewGuid();
                    testObjects.Add(g);
                }

                // Put the objects
                Parallel.ForEach(testObjects, g =>
                    {
                        var ok = client.PutObject(g.ToString(), g.ToString());
                        //Console.WriteLine("Put object {0}: {1}",
                        //    g.ToString(), ok);
                        if (!ok) allTestsPassed = false;
                    });

                Console.WriteLine("Done putting objects");

                if (!allTestsPassed)
                {
                    Console.WriteLine("Some puts failed");
                    return;
                }

                // Delete the objects
                Parallel.ForEach(testObjects, g =>
                {
                    var ok = client.DeleteObject(g.ToString());
                    //Console.WriteLine("Deleted object {0}: {1}",
                    //    g.ToString(), ok);
                    if (!ok) allTestsPassed = false;
                });

                Console.WriteLine("Done deleting objects");

                if (!allTestsPassed)
                {
                    Console.WriteLine("Some deletes failed");
                    return;
                }

                if (allTestsPassed)
                {
                    Console.WriteLine("All puts and deletes succeeded");
                }
            }
            else if (args.Length >= 1 && args[0].ToLower().Equals("-reperf"))
            {
                CacheHelper.RemovePerformanceCounters();
                CacheHelper.InitPerformanceCounters();
            }
            else if (args.Length >= 1 && args[0].ToLower().Equals("-automaster"))
            {
                CacheConfig config = AutoConfigMaster();
                Console.WriteLine("About to start server with these settings: {0}",
                        config.GetTrace());

                CacheHelper.InitPerformanceCounters();

                CacheListener listener = new CacheListener(config);
                var task = listener.StartAsync();

                Console.WriteLine("Press Enter to stop");
                Console.ReadLine();
                listener.Stop();

                Console.WriteLine("After Stop");

                bool result = task.Result;

                Console.WriteLine("Got result {0}", result);
            }
            else if (args.Length >= 1 && args[0].ToLower().Equals("-autonode"))
            {
                int portNumber = int.Parse(args[1]);

                CacheConfig config = AutoConfigNode(portNumber);
                Console.WriteLine("About to start server with these settings: {0}",
                        config.GetTrace());

                CacheHelper.InitPerformanceCounters();

                CacheListener listener = new CacheListener(config);
                var task = listener.StartAsync();

                Console.WriteLine("Press Enter to stop");
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

        static CacheConfig AutoConfigMaster()
        {
            string localHost = "localhost";

            CacheConfig config = new CacheConfig();
            //
            //  Master
            //  
            config.MasterHostName = localHost;
            config.MasterPortNumber = 12345;
            config.Ring = new CacheRing();
            //
            //  Listener
            //
            config.ListenerHostName = localHost;
            config.ListenerIP = "127.0.0.1";
            config.ListenerPortNumber = 12345;
            config.IsMaster = true;
            //
            //  Trace
            //
            config.IsTraceEnabled = false;
            config.TraceFilePath = "D:\\CacheSupport\\Logs\\Master_12345.txt";

            return config;
        }

        static CacheConfig AutoConfigNode(int portNumber)
        {
            string localHost = "localhost";

            CacheConfig config = new CacheConfig();
            //
            //  Master
            //  
            config.MasterHostName = localHost;
            config.MasterPortNumber = 12345;
            config.ListenerHostName = localHost;
            config.ListenerIP = "127.0.0.1";
            config.ListenerPortNumber = portNumber;
            config.IsMaster = false;
            //
            //  Trace
            //
            config.IsTraceEnabled = false;
            config.TraceFilePath = string.Format( "D:\\CacheSupport\\Logs\\Node_{0}.txt", portNumber);

            return config;
        }

        //static void AutoConfigMasterNode(CacheConfig config, int portNumber, double memModifier)
        //{
        //    CacheNode node = new CacheNode();
        //    node.HostName = "localhost";
        //    node.IPEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), portNumber);
        //    node.MaxNumBytes = 
        //    node.PortNumber = portNumber;
        //    node.Status = CacheNodeStatus.Up;
        //    config.Ring.AddNode(node);
        //}

		static void Usage()
		{
			Console.WriteLine("LoopCacheConsole.exe");
			Console.WriteLine("\t-help\t\t\t\tShow this message");
			Console.WriteLine("\t-test\t\t\t\tRun some basic unit tests");
			Console.WriteLine("\t-testclient hostname:port\tUnit test client requests.  ");
			Console.WriteLine("\t\t\t\t\tRequires master and data nodes to be running."); 
			Console.WriteLine("\t\t\t\t\thostname:port is the master listener");
			Console.WriteLine("\t-server config.txt\t\tRun a master or data node");
            Console.WriteLine("\t-add masterHost:port newNodeHost:port maxNumBytes\t" + 
                "Add a new node to the cluster");
            Console.WriteLine("\t-list masterHost:port\tList the data nodes in the cluster");
            Console.WriteLine("\t-testservice host:port\tUnit test the windows service with a simplified set of tests that don't require other nodes.");
		}

		static bool TestCacheRing()
		{
            try
            {
                return CacheRing.Test();
            }
            catch (Exception ex)
            {
                Console.WriteLine("CacheRing test failed: " + ex.Message);
                return false;
            }
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
