using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace LoopCacheLib
{
    /// <summary>Contains helper methods for the cache lib</summary>
    public static class CacheHelper
    {
        /// <summary>
        /// If this is non-null, trace is enabled and will be written to this path
        /// </summary>
        public static string TraceFilePath { get; set; }

        /// <summary>
        /// Convert the string to an integer representation of a consistent md5 hash.
        /// </summary>
        /// <remarks>Collisions are possible, but they don't matter because we only 
        /// want an even distribution across the range of ints for a random 
        /// assortment of strings.  This method returns the same int for the same string 
        /// on any platform.  DON'T CHANGE IT!</remarks>
        /// <param name="s">The string to hash</param>
        /// <returns>The consistent hash code</returns>
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

        /// <summary>
        /// Clients call this method to send a message to a listener.
        /// </summary>
        /// <remarks>Shouldn't be used by the listener since it's not async.</remarks>
        /// <param name="request"></param>
        /// <param name="serverAddress"></param>
        /// <returns></returns>
        public static CacheMessage SendRequest(CacheMessage request, IPEndPoint serverAddress)
        {
            using (TcpClient client = new TcpClient())
            {
                client.Connect(serverAddress);

                using (NetworkStream stream = client.GetStream())
                {
                    MessageToStream(request, stream);
                    return MessageFromStream(stream);
                }
            }
        }

        /// <summary>Write a message to a stream</summary>
        private static void MessageToStream(CacheMessage m, Stream s)
        {
            BinaryWriter w = new BinaryWriter(s);
            w.Write(m.MessageType);
            w.Write(IPAddress.HostToNetworkOrder(m.MessageLength));
            if (m.MessageLength > 0)
            {
                w.Write(m.Data);
            }
            w.Flush();
        }

        /// <summary>Read a message from a stream</summary>
        private static CacheMessage MessageFromStream(Stream s)
        {
            BinaryReader r = new BinaryReader(s);
            byte responseType = r.ReadByte();

            int responseLength = IPAddress.NetworkToHostOrder(r.ReadInt32());
            byte[] responseData = r.ReadBytes(responseLength);

            CacheMessage m = new CacheMessage();
            m.MessageType = responseType;
            m.Data = responseData;

            return m;
        }

        private static object traceLock = new object();

        /// <summary>
        /// Logs a trace message to a text file for debugging and troubleshooting.
        /// </summary>
        /// <remarks>This overload acts like string.Format</remarks>
        public static void LogTrace(string messageFormat, params object[] args)
        {
            try
            {
                LogTrace(string.Format(messageFormat, args));
            }
            catch { }
        }

        /// <summary>
        /// Logs a trace message to a text file for debugging and troubleshooting.
        /// </summary>
        public static void LogTrace(string message)
        {
            if (TraceFilePath == null) return;

            // we only want to write one message at a time
            lock (traceLock)
            {
                try
                {
                    using (StreamWriter sw = new StreamWriter(TraceFilePath, true))
                    {
                        sw.WriteLine(DateTime.Now.ToString("yyyyMMdd HH':'mm':'ss'.'fff") + 
                            " - " + message);
                    }
                }
                catch (Exception ex)
                {
                    // Conundrum: if there's a problem logging, where should we log that...

                    // This can help when testing code from the command line.
                    // It will get discarded in a windows service.
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        /// <summary></summary>
        public static void LogInfo(string message)
        {
            LogEvent(message, EventLogEntryType.Information);
        }

        /// <summary></summary>
        public static void LogWarning(string message)
        {
            LogEvent(message, EventLogEntryType.Warning);
        }

        /// <summary></summary>
        public static void LogError(string message)
        {
            LogEvent(message, EventLogEntryType.Error);
        }

        /// <summary></summary>
        public static void LogError(string message, Exception ex)
        {
            LogError(string.Format("{0}: {1}", 
                message, 
                ex == null ? "[null]" : ex.ToString()));
        }

        /// <summary></summary>
        public static void LogEvent(string message, EventLogEntryType errorType)
        {
            try
            {
                LogTrace(string.Format("{0}: {1}", errorType, message));

                using (EventLog e = new EventLog("Application", ".", "LoopCache"))
                {
                    e.WriteEntry(message, errorType);
                    e.Close();
                }

            }
            catch
            {
            }
        }

        /// <summary></summary>
        public static IPEndPoint GetIPEndPoint(string hostname, int port)
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

    }
}

