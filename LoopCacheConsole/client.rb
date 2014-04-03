# This is a Ruby client for the Loop Cache.
#
# It's a proof-of-concept to show that any language can use the cache
# as a substitute for memcached in Windows environments.

require "digest"
require "resolv"

class LoopCacheClient
    
    # Request Types

    REQUEST_GET_CONFIG         = 1;
    REQUEST_NODE_DOWN          = 2;
    REQUEST_ADD_NODE           = 3;
    REQUEST_REMOVE_NODE        = 4;
    REQUEST_CHANGE_NODE        = 5;
    REQUEST_GET_STATS          = 6;
    REQUEST_GET_OBJECT         = 7;
    REQUEST_PUT_OBJECT         = 8;
    REQUEST_DELETE_OBJECT      = 9;
    REQUEST_CHANGE_CONFIG      = 10;
    REQUEST_REGISTER           = 11;
    REQUEST_PING               = 12;

    # Response Types

    RESPONSE_INVALID_REQUEST_TYPE     = 1;
    RESPONSE_NOT_MASTER_NODE          = 2; 
    RESPONSE_NOT_DATA_NODE            = 3; 
    RESPONSE_OBJECT_OK                = 4; 
    RESPONSE_OBJECT_MISSING           = 5;
    RESPONSE_RECONFIGURE              = 6;
    RESPONSE_CONFIGURATION            = 7;
    RESPONSE_INTERNAL_SERVER_ERROR    = 8;
    RESPONSE_READ_KEY_ERROR           = 9;
    RESPONSE_READ_DATA_ERROR          = 10;
    RESPONSE_UNKNOWN_NODE             = 11;
    RESPONSE_ENDPOINT_MISMATCH        = 12;
    RESPONSE_NODE_EXISTS              = 13;
    RESPONSE_ACCEPTED                 = 14;
    RESPONSE_DATA_NODE_NOT_READY      = 15;

    # int => LoopCacheNode
    @locations = {}

    # "host:port" => LoopCacheNode
    @nodes = {}

    # Initialize an instance with the hostname and port of the master node
    def initialize (master_hostname, master_port)
        @master_ip = Resolv.getaddress(hostname)
        @master_port = master_port
    end

    # Get a consistent hash code that matches the algorithm
    # used by the Loop Cache server's C# code.  This will be 
    # used to correctly predict which node to choose for the key.
    def self.get_consistent_hash (key)

        # Get the MD5 digest
        d = Digest::MD5.digest(key)

        # Create ints from each 4-byte sequence
        d0 = d[0..3].unpack("l") # lower case l for signed int32
        d1 = d[4..7].unpack("l")
        d2 = d[8..11].unpack("l")
        d3 = d[12..15].unpack("l")

        # Combine these into a single int
        d0[0] ^ d1[0] ^ d2[0] ^ d3[0]
    end

    # Get the data node that owns this key.
    # Virtual nodes are on a 32-bit ring.  Find the location
    # of the key's hash and the node that owns it is the first node
    # we find from that point upwards.
    def get_node_for_key (key)
        h = get_consistent_hash(key)
        sorted_locations = @locations.sort_by { |key,value| key }
        first_node = nil
        sorted_locations.each { |kvp|
            if first_node == nil
                first_node = kvp[1]
            end
            if kvp[0] >= h
                return kvp[1]
            end
        }
        return first_node;
    end

    # Get the ring configuration from the master node
    def get_config
        # TODO
    end

    # Send a message to a node
    def send_message(server, message_type, data)
        # TODO
        s = TCPSocket.open(@master_ip, @master_port)

        # host to network order: e.g. [1024].pack("N")


    end


end

class LoopCacheNode
    attr_accessor :host, :port, :max_num_bytes
end
