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
        /// <summary>
        /// True if the process should log trace messages
        /// </summary>
        public bool IsTraceEnabled { get; set; }

        /// <summary>
        /// Full path to the trace log file
        /// </summary>
        public string TraceFilePath { get; set; }

        /// <summary>
        /// True if this is a master node
        /// </summary>
        public bool IsMaster { get; set; }

        /// <summary>
        /// The cache ring, with node configuration for the whole cluster
        /// </summary>
        public CacheRing Ring { get; set; }

        /// <summary>
        /// The host name for this process
        /// </summary>
        public string ListenerHostName { get; set; }

        /// <summary>
        /// The IP for this process to listen on
        /// </summary>
        public string ListenerIP { get; set; }

        /// <summary>
        /// The port number for this process to listen on
        /// </summary>
        public int ListenerPortNumber { get; set; }

        /// <summary>
        /// The host name of the master node
        /// </summary>
        public string MasterHostName { get; set; }

        /// <summary>
        /// The port number for the master node
        /// </summary>
        public int MasterPortNumber { get; set; }

        /// <summary>
        /// The IP end point for the master node
        /// </summary>
        public IPEndPoint MasterIPEndPoint { get; set; }

        /// <summary>
        /// Returns true if this is a data node
        /// </summary>
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
                        CacheNode n = new CacheNode();
                        n.HostName = this.MasterHostName;
                        n.PortNumber = this.MasterPortNumber;
                        if (this.Ring.FindNodeByName(n.GetName()) != null)
                        {
                            return true;
                        }
                        else
                        {
                            return false;
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

        /// <summary>
        /// Create a new instance of the cache config.
        /// </summary>
        public CacheConfig()
        {
            this.ConfigLines = new SortedList<int, ConfigLine>();
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
        /// The lines of the config file.
        /// </summary>
        public SortedList<int, ConfigLine> ConfigLines { get; set; }

        /// <summary>
        /// Parse the config file.
        /// </summary>
        public static CacheConfig Load(string fileName)
        {
            CacheConfig config = new CacheConfig();
            CacheRing ring = new CacheRing();
            config.Ring = ring; // Only gets populated for the master node

            int lineNum = 0;
            using (StreamReader sr = new StreamReader(fileName))
            {
                string line = null;
                while (true)
                {
                    line = sr.ReadLine();
                    if (line == null) break;
                    lineNum++;
                    ConfigLine configLine = new ConfigLine();
                    configLine.Line = line;
                    config.ConfigLines.Add(lineNum, configLine);

                    line = line.Trim().ToLower();
                    if (line.Equals(String.Empty)) continue;
                    if (line.StartsWith("#")) continue;

                    string[] tokens = SplitLine(line);

                    if (line.StartsWith("node"))
                    {
                        CacheNode node = ParseNodeLine(line);
                        ring.AddNode(node);
                    }
                    else if (line.StartsWith("listener"))
                    {
                        configLine.IsNodeLine = true;

                        // # Listener        host        ip:port                IsMaster Yes|No
                        // Listener            localhost    127.0.0.1:12345        Yes

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
                        // # Trace        On|Off        File
                        // Trace        On            C:\Loop\Logs\LoopCacheMaster.txt

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
                        // # Master    host:port            
                        // Master        localhost:12345    

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

        /// <summary>
        /// Fix the config lines to reflect the current ring configuration, 
        /// maintaining the original file as closely as possible.
        /// </summary>
        private void FixConfigLines()
        {
            SortedList<int, ConfigLine> newLines = new SortedList<int, ConfigLine>();

            bool wroteNewNodes = false;

            int newLineNum = 0;
            foreach (var oldLine in this.ConfigLines.Values)
            {
                if (oldLine.IsNodeLine)
                {
                    if (wroteNewNodes == false)
                    {
                        foreach (var node in this.Ring.Nodes.Values)
                        {
                            // Node	localhost:12346		24Mb
                            string nodeStr = string.Format("Node\t{0}:{1}\t{2}",
                                node.HostName, node.PortNumber, 
                                GetFriendlyFileSize(node.MaxNumBytes));
                            newLineNum++;
                            newLines.Add(newLineNum, new ConfigLine(nodeStr, true));
                        }
                    }

                    // Ignore all the old node lines
                }
                else
                {
                    newLineNum++;
                    newLines.Add(newLineNum, oldLine);
                }
            }

            // Replace the old lines with the new ones
            this.ConfigLines = newLines;
        }

        /// <summary>
        /// Saves the configuration to file.
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="config"></param>
        public static void Save(string fileName, CacheConfig config)
        {
            config.FixConfigLines();

            using (StreamWriter sw = new StreamWriter(fileName))
            {
                foreach (var line in config.ConfigLines.Values)
                {
                    sw.WriteLine(line.Line);
                }
            }
        }

        /// <summary>Split a line on spaces and tabs, removing empty entries</summary>
        public static string[] SplitLine(string line)
        {
            // Split the line up on white space and remove empty entries
            char[] charSeparators = new char[] { ' ', '\t' };
            string[] tokens = line.Split(charSeparators,
                    StringSplitOptions.RemoveEmptyEntries);
            return tokens;
        }

        /// <summary>Parse a line of the config file with a Node configuration</summary>
        public static CacheNode ParseNodeLine(string line)
        {
            string[] tokens = SplitLine(line);

            if (tokens.Length != 3)
            {
                throw new Exception("Invalid configuration line: " + line);
            }

            // # Node        host:port            MaxMem
            // Node            localhost:12346        24Mb

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

        /// <summary>
        /// If the size is an even multiple of 1024, return Kb, Mb, or Gb
        /// </summary>
        /// <param name="size">The number of bytes</param>
        /// <returns>A string representing the size, like "24Gb"</returns>
        public static string GetFriendlyFileSize(long size)
        {
            if (size % 1024 != 0)
            {
                return size.ToString();
            }

            int kb = 1024;
            int mb = kb * 1024;
            int gb = mb * 1024;

            if (size % gb == 0)
            {
                return string.Format("{0}Gb", size / gb);
            }

            if (size % mb == 0)
            {
                return string.Format("{0}Mb", size / mb);
            }

            if (size % kb == 0)
            {
                return string.Format("{0}Kb", size / kb);
            }

            throw new Exception("Unexpected size"); // Is this possible?
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

    /// <summary>
    /// Represents a line in the config file.
    /// </summary>
    public class ConfigLine
    {
        /// <summary>
        /// The original line
        /// </summary>
        public string Line { get; set; }

        /// <summary>
        /// True if this is a Node line
        /// </summary>
        public bool IsNodeLine { get; set; }

        /// <summary>
        /// Create a new default instance.
        /// </summary>
        public ConfigLine()
        { }

        /// <summary>
        /// Create an instance based on values.
        /// </summary>
        /// <param name="line"></param>
        /// <param name="isNodeLine"></param>
        public ConfigLine(string line, bool isNodeLine)
        {
            this.Line = line;
            this.IsNodeLine = IsNodeLine;
        }
    }
}

