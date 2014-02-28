using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace LoopCacheLib
{
	/// <summary>Contains helper methods for loading ring configuration</summary>
	public class CacheConfig
	{
		public bool IsTraceEnabled { get; set; }
		public string TraceFilePath { get; set; }
		public bool IsMaster { get; set; }
		public CacheRing Ring { get; set; }
		public string ListenerHostName { get; set; }
		public string ListenerIP { get; set; }
		public int ListenerPortNumber { get; set; }
		public string MasterHostName { get; set; }
		public int MasterPortNumber { get; set; }
		public IPEndPoint MasterIPEndPoint { get; set; }

		public bool IsDataNode
		{
			get
			{
				if (this.MasterHostName.Equals(this.ListenerHostName) &&
					this.MasterPortNumber == this.ListenerPortNumber)
				{
					// It's possible for the master node to be a data node, 
					// but not generally recommended for production.
					if (this.Ring != null)
					{
						foreach (var node in this.Ring.Nodes.Values)
						{
							if (node.HostName.Equals(this.MasterHostName) && 
								node.PortNumber.Equals(this.MasterPortNumber))
							{
								return true;
							}
						}
					}
					return false;
				}
				else
				{
					return true;
				}
			}
		}

		/// <summary>Get a detailed description of the config</summary>
		public string GetTrace()
		{
			StringBuilder sb = new StringBuilder();

			sb.AppendFormat("IsTraceEnabled: {0}\r\n", this.IsTraceEnabled);
			sb.AppendFormat("TraceFilePath: {0}\r\n", this.TraceFilePath);
			sb.AppendFormat("IsMaster: {0}\r\n", this.IsMaster);
			sb.AppendFormat("ListenerHostName: {0}\r\n", this.ListenerHostName);
			sb.AppendFormat("ListenerIP: {0}\r\n", this.ListenerIP);
			sb.AppendFormat("ListenerPortNumber: {0}\r\n", this.ListenerPortNumber);
			sb.AppendFormat("MasterHostName: {0}\r\n", this.MasterHostName);
			sb.AppendFormat("MasterPortnumber: {0}\r\n", this.MasterPortNumber);
			sb.AppendFormat("Ring: {0}\r\n", this.Ring.GetTrace());

			return sb.ToString();
		}

		/// <summary>
		/// Parse the config file.
		/// </summary>
		/// <remarks>Does not figure out node locations</remarks>
		public static CacheConfig Load(string fileName)
		{
			CacheConfig config = new CacheConfig();
			CacheRing ring = new CacheRing();
			config.Ring = ring; // Only gets populated for the master node

			using (StreamReader sr = new StreamReader(fileName))
			{
				string line = null;
				while (true)
				{
					line = sr.ReadLine();
					if (line == null) break;
					line = line.Trim().ToLower();
					if (line.Equals(String.Empty)) continue;
					if (line.StartsWith("#")) continue;

					string[] tokens = SplitLine(line);

					if (line.StartsWith("node"))
					{
						CacheNode node = ParseNodeLine(line);
						string nodeName = node.GetName();
						if (ring.Nodes.ContainsKey(nodeName))
						{
							throw new Exception("Already added node " + nodeName);
						}
						ring.Nodes.Add(nodeName, node);
					}
					else if (line.StartsWith("listener"))
					{
						// # Listener		host		ip:port				IsMaster Yes|No
						// Listener			localhost	127.0.0.1:12345		Yes

						if (tokens.Length != 4)
						{
							throw new Exception("Invalid config line: " + line);
						}

						config.ListenerHostName = tokens[1];
						string ipPort = tokens[2];
						string[] ipPortTokens = ipPort.Split(':');
						if (ipPortTokens.Length != 2)
						{
							throw new Exception("Invalid config line: " + line);
						}
						config.ListenerIP = ipPortTokens[0];
						int lpn;
						if (!int.TryParse(ipPortTokens[1], out lpn))
						{
							throw new Exception("Invalid config line: " + line);
						}
						config.ListenerPortNumber = lpn;
						if (tokens[3].Equals("yes")) 
						{
							config.IsMaster = true;
						}
						else if (tokens[3].Equals("no"))
						{
							config.IsMaster = false;
						}
						else
						{
							throw new Exception(
								"Invalid config line, IsMaster should be Yes or No: " + line);
						}
					} 
					else if (line.StartsWith("trace"))
					{
						// # Trace		On|Off		File
						// Trace		On			C:\Loop\Logs\LoopCacheMaster.txt

						if (tokens.Length != 3)
						{
							throw new Exception("Invalid config line: " + line);
						}

						if (tokens[1].Equals("on"))
						{
							config.IsTraceEnabled = true;
						}
						else if (tokens[1].Equals("off"))
						{
							config.IsTraceEnabled = false;
						}
						else
						{
							throw new Exception(
									"Invalid config line, Trace should be On of Off: " + line);
						}

						config.TraceFilePath = tokens[2];
					}
					else if (line.StartsWith("master"))
					{
						// # Master	host:port			
						// Master		localhost:12345	

						if (tokens.Length != 2)
						{
							throw new Exception(string.Format(
								"Invalid config line, {0} tokens: {1}", tokens.Length, line));
						}

						string[] hostPortTokens = tokens[1].Split(':');
						if (hostPortTokens.Length != 2)
						{
							throw new Exception("Invalid config line, host:port: " + line);
						}

						config.MasterHostName = hostPortTokens[0];
						int mpn;
						if (!int.TryParse(hostPortTokens[1], out mpn))
						{
							throw new Exception("Invalid config line, port number: " + line);
						}
						config.MasterPortNumber = mpn;
					}
				}
			}

			return config;
		}

		public static string[] SplitLine(string line)
		{
			// Split the line up on white space and remove empty entries
			char[] charSeparators = new char[] {' ', '\t'};
			string[] tokens = line.Split(charSeparators, 
					StringSplitOptions.RemoveEmptyEntries);
			return tokens;
		}

		public static CacheNode ParseNodeLine(string line)
		{
			string[] tokens = SplitLine(line);

			if (tokens.Length != 3)
			{
				throw new Exception("Invalid configuration line: " + line);
			}

			// # Node		host:port			MaxMem
			// Node			localhost:12346		24Mb

			string hostPortStr = tokens[1];
			string maxMemStr = tokens[2];

			CacheNode node = new CacheNode();

			node.MaxNumBytes = ParseMaxNumBytes(maxMemStr);

			string[] hostPortTokens = hostPortStr.Split(':');

			if (hostPortTokens.Length != 2)
			{
				throw new Exception(
					"Invalid configuration line (host:port): " + line);
			}

			node.HostName = hostPortTokens[0];
			int portNumber;
			if (!int.TryParse(hostPortTokens[1], out portNumber))
			{
				throw new Exception(
					"Invalid configuration line (port): " + line);
			}
			node.PortNumber = portNumber;

			return node;
		}

		/// <summary>Parse the MaxNumBytes setting from the config file</summary>
		/// <remarks>e.g. 1024 or 1,002,003 or 1024Kb or 11Mb or 24Gb</remarks>
		/// <returns>The number of bytes</returns>
		public static long ParseMaxNumBytes(string s)
		{
			if (s == null) throw new ArgumentException("s is null");

			s = s.Trim().ToLower();
			
			if (s.Equals(String.Empty)) throw new ArgumentException("s is empty");

			// It might be the raw number of bytes, with or without commas
			s = s.Replace(",", "");
			long b;
			if (long.TryParse(s, out b))
			{
				return b;
			}

			int multiplyBy = 0;
			string snum = null;
			if (s.IndexOf("kb") > -1)
			{
				snum = s.Replace("kb", "");
				multiplyBy = 1024;
			}
			else if (s.IndexOf("mb") > -1)
			{
				snum = s.Replace("mb", "");
				multiplyBy = 1024 * 1024;
			}
			else if (s.IndexOf("gb") > -1)
			{
				snum = s.Replace("gb", "");
				multiplyBy = 1024 * 1024 * 1024;
			}
			else
			{
				throw new ArgumentException("Invalid MaxMem: " + s);
			}

			if (long.TryParse(snum, out b))
			{
				return b * multiplyBy;
			}
			else
			{
				throw new ArgumentException("Invalid MaxMem: " + s);
			}
			
		}

		/// <summary>
		/// 127.0.0.1:12345
		/// </summary>
		/// <param name="ipPort"></param>
		/// <param name="ipEndPoint"></param>
		/// <returns></returns>
		public static bool TryParseIPEndPoint(string ipPort, out IPEndPoint ipEndPoint)
		{
			ipEndPoint = null;
			try
			{
				string[] toks = ipPort.Split(':');
				if (toks.Length != 2)
				{
					return false;
				}

				ipEndPoint = new IPEndPoint(IPAddress.Parse(toks[0]), int.Parse(toks[1]));
				return true;
			}
			catch (Exception)
			{
				return false;
			}
		}
	}
}

