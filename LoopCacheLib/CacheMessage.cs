using System;
using System.Text;

namespace LoopCacheLib
{
	/// <summary>
	/// All messages passed over the network consist of a type, length, and data
	/// </summary>
	public class CacheMessage
	{
		public byte MessageType { get; set; }

		public int MessageLength 
		{ 
			get
			{
				if (this.Data == null) return 0;
				return this.Data.Length;
			}
		}

		public byte[] Data { get; set; }

		/// <summary>A convenience property for converting Data to and from a string</summary>
		/// <remarks>Be careful not to call this unless you're sure Data is a string</remarks>
		public string StringData
		{
			get
			{
				return Encoding.UTF8.GetString(this.Data);
			}
			set
			{
				this.Data = Encoding.UTF8.GetBytes(value);
			}
		}
	}

	public enum CacheRequestTypes : byte
	{
		/// <summary>A request for the node's current configuration</summary>
		GetConfig			= 1,

		/// <summary>A report from a client that a node is not responding</summary>
		NodeDown		 	= 2,

		/// <summary>A request for the master node to add a data node and push the
		/// change out to all data nodes.</summary>
		AddNode		  		= 3,

		/// <summary>A request for the master node to remove a data node and push the
		/// change out to all data nodes.</summary>
		RemoveNode	   		= 4,

		/// <summary>A request for the master node to make a change to a node and push the
		/// change out to all data nodes</summary>
		ChangeNode	   		= 5,

		/// <summary>A request for node statistics</summary>
		GetStats		 	= 6,

		/// <summary>A request from a client to a data node to retrieve a cached object
		/// </summary>
		GetObject			= 7,

		/// <summary>A request from a client to a data node to store an object in the cache.
		/// </summary>
		PutObject			= 8,

		/// <summary>A request from a client to a data node to remove an object from the cache
		/// </summary>
		DeleteObject	 	= 9,

		/// <summary>A request from the master to a data node to change its configuration.
		/// </summary>
		ChangeConfig	 	= 10, 

		/// <summary>A request from a data node to the master node to say it's alive and to 
		/// get the configuration.</summary>
		Register			= 11
	}

	public enum CacheResponseTypes : byte
	{
		/// <summary>The request type was not recognized</summary>
		InvalidRequestType 	= 1, 

		/// <summary>The request type is for the master node, and I am a data node</summary>
		NotMasterNode = 2, 

		/// <summary>The request type is for a data node, and I am the master</summary>
		NotDataNode = 3, 

		/// <summary>You made successful request for an object, here it is</summary>
		ObjectOk = 4, 

		/// <summary>I am responsible for that object, but I don't have it</summary>
		ObjectMissing = 5,

		/// <summary>You asked me for something I'm not responsible for, here is the config
		/// </summary>
		ReConfigure = 6,

        /// <summary>You made a successfuly request for configuration, here it is</summary>
        Configuration = 7, 

		/// <summary>An unexpected error happened in the server</summary>
		InternalServerError = 8, 

		/// <summary>Key data looks corrupted<summary>
		ReadKeyError = 9, 

		/// <summary>Data looks corrupted</summary>
		ReadDataError = 10
	}
}
