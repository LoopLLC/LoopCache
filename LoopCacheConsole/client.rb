# This is a Ruby client for the Loop Cache.
#
# It's a proof-of-concept to show that any language can use the cache
# as a substitute for memcached in Windows environments.

require "digest"
require "resolv"

class LoopCacheClient
    
    # int => LoopCacheNode
    @locations = {}

    # "host:port" => LoopCacheNode
    @nodes = {}

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

    def get_node_for_key (key)
        h = get_consistent_hash(key)
        sorted_locations = @locations.sort_by { |key,value| key }
        # TODO
    end

    def get_config

    end


end
