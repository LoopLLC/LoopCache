using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace LoopCache.Client
{
    public static class Common
    {
        private static int MaxLength = 1024 * 1024; // 1Mb
        private static Dictionary<string, int> hashCodes = new Dictionary<string, int>();

        /// <summary>
        /// Convert the string to an integer representation of a consistent md5 hash.
        /// </summary>
        /// <remarks>Collisions are possible, but they don't matter because we only 
        /// want an even distribution across the range of ints for a random 
        /// assortment of strings.  This method returns the same int for the same string 
        /// on any platform.  DON'T CHANGE IT!
        ///
        /// This is copied right out of LoopCacheLib.  It needs to work exactly the same
        /// in both places.
        ///
        /// If you implement this in another language, make sure it generates hash codes
        /// that match what you get from C#.
        ///
        /// This method is used to predict which data node we should be talking to.
        ///
        /// </remarks>
        /// <param name="guid"></param>
        /// <returns></returns>
        public static int GetConsistentHashCode(string s)
        {
            if (s == null) return 0;

            if (hashCodes.ContainsKey(s))
                return hashCodes[s];

            MD5 md5 = MD5.Create();
            byte[] hash = md5.ComputeHash(Encoding.ASCII.GetBytes(s));
            int a = BitConverter.ToInt32(hash, 0);
            int b = BitConverter.ToInt32(hash, 4);
            int c = BitConverter.ToInt32(hash, 8);
            int d = BitConverter.ToInt32(hash, 12);

            int hashCode = (a ^ b ^ c ^ d);
            hashCodes[s] = hashCode;

            return hashCode;
        }

        /// <summary>
        /// Look up the host and create an IP end point based on the hostname and port.
        /// </summary>
        /// <param name="hostname"></param>
        /// <param name="port"></param>
        /// <returns></returns>
        public static IPEndPoint GetIPEndPoint(string hostname, int port)
        {
            IPEndPoint returnValue = null;

            IPAddress[] ips = Dns.GetHostAddresses(hostname);

            foreach (IPAddress ip in ips)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    returnValue = new IPEndPoint(ip, port);
                }
            }

            if (returnValue == null)
                throw new Exception("Unable to resolve address");

            return returnValue;
        }

        public static Response SendMessage(string hostname, int port, Request request)
        {
            IPEndPoint ipep = Common.GetIPEndPoint(hostname, port);

            if (ipep != null)
            {
                using (TcpClient client = new TcpClient())
                {
                    try
                    {
                        client.Connect(ipep);

                        using (NetworkStream stream = client.GetStream())
                        {
                            Common.WriteRequest(stream, request);
                            return Common.ReadResponse(stream);
                        }
                    }
                    catch (SocketException)
                    {
                    }
                }             
            }

            return null;
        }

        public static byte[] ToByteArray(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        public static string FromByteArray(byte[] b)
        {
            return Encoding.UTF8.GetString(b);
        }

        public static byte[] ToByteArray(object o)
        {
            if (o == null)
            {
                return null;
            }
            var bf = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                bf.Serialize(ms, o);
                return ms.ToArray();
            }
        }

        public static T FromByteArray<T>(byte[] b)
        {
            if (b == null)
            {
                return default(T);
            }
            using (var memStream = new MemoryStream())
            {
                var binForm = new BinaryFormatter();
                memStream.Write(b, 0, b.Length);
                memStream.Seek(0, SeekOrigin.Begin);
                var obj = (T)binForm.Deserialize(memStream);
                return obj;
            }
        }

        public static void WriteRequest(NetworkStream stream, Request request)
        {
            BinaryWriter w = new BinaryWriter(stream);
            w.Write((byte)request.Type);
            if (request.Data == null)
            {
                w.Write(IPAddress.HostToNetworkOrder((int)0));
            }
            else
            {
                w.Write(IPAddress.HostToNetworkOrder(request.Data.Length));
                w.Write(request.Data);
            }
            w.Flush();
        }

        public static Response ReadResponse(NetworkStream stream)
        {
            BinaryReader r = new BinaryReader(stream);
            Response.Types responseType = (Response.Types)r.ReadByte();
            int responseLength = IPAddress.NetworkToHostOrder(r.ReadInt32());
            if (responseLength > MaxLength)
            {
                throw new Exception(
                    "SendMessage got a response that exceeded max length");
            }
            byte[] responseData = new byte[0];
            if (responseLength > 0)
            {
                responseData = r.ReadBytes(responseLength);
            }

            return new Response(responseType, responseData);
        }

        public static string Read(byte[] b)
        {
            return Encoding.UTF8.GetString(b);
        }

        public static int Read(int i)
        {
            return IPAddress.NetworkToHostOrder(i);
        }

        public static long Read(long i)
        {
            return IPAddress.NetworkToHostOrder(i);
        }
    }
}
