LoopCache
=========

A distributed key-value memory store, similar to memcached, written in C#

Eric Z. Beard
eric@loopfx.com

NOTE: This is not even close to functional yet!  It compiles, and some basic tests pass.  That's it.

## High Level Requirements

- Store byte arrays keyed by a string
- Distribute the keys across several data nodes
- Data nodes do not have to be the same size
- Specify maximum RAM usage, purge LRU when full
- Use extra RAM on middleware machines
- Low CPU utilization
- Consistent hashing
- Rebalance when a node is added or removed
- No dependencies
- Run as a Windows Service or from the console
- Clients can be written in any language
- Custom binary protocol
- Objects are not persisted to disk.  It's a cache, not a database.

## Consistent Hashing

I would suggest a quick google search for consistent hashing, there are some good articles out there that explain it pretty well.  

The idea is that you want to spread objects out over a cluster of cache machines in a way that their assigned locations are somewhat stable when a node is added or removed.  We could use a simplistic approach and use a modulus of the hashed key like this:

location = hash(key) % numServers

But if we add or remove a node, most of the locations will change, invalidating nearly the entire cache.  By using consistent hashing, the number of keys that need to be moved is roughly the size of the newly added node's capacity.  We do this by creating a large number of virtual node locations on a 32-bit ring.  We hash a key and look for the next highest location on the ring to detemine which node owns that key.

Here's a simple visualization for a 3-node cluster, with nodes labelled A, B, and C:

IntMin.....A...B..A..C..C..B..A..A..B..C....A....B..C..B..A..C..B..A..A..B..B..C..A..B....IntMax

If you remove a node, objects under the removed locations simply shift up to the next node.  Adding a node shifts objects under the new locations to the new node.

## Binary Protocol

The protocol is pretty simple.  It's a single byte for the message type (which API method you want to call), then an int for the length of the data, and then then data itself.

[0]     Message Type
[1-4]    Length
[5-X]    Data

Some methods have their own formats for the Data, which are also generally pretty simple.  The pattern is usually to have a 32 bit integer for the length of any arbitrary data, then the data bytes.  Strings are always UTF8 encoded.

Q: Why not use JSON?
A: Because I don't want any dependencies on third party libraries, and I want this thing to be as fast as possible.

See LoopCacheLib\SampleCacheClient.cs for an example of how to talk to the server.

### Requests

The number in parantheses is the preceding byte for the message.  

- GetConfig (1)

    Any node will respond.  Only master is totally reliable.

    Responds with *Configuration* (see below)

    Request Layout:

        MessageType        byte (1)
        DataLength         int (0, meaning no data)


- NodeDown             (2)
    
    Master only.  Clients report nodes that aren't responding.
    
    Request Layout:

        MessageType     byte (2)
        DataLength      int
        Data            byte[]
            HostLen         int
            Host            byte[] UTF8 string
            Port            int

- AddNode            (3)

    Master only.  From admin console.

    Request Layout:

        HostLen         int
        Host            byte[] UTF8 string
        Port            int
        MaxNumBytes     long

- RemoveNode        (4)

    Master only.  From admin console.

    Request Layout:

        HostLen         int
        Host            byte[] UTF8 string
        Port            int

- ChangeNode        (5)

    Master only.  From admin console.
    
    Request Layout:

        HostLen         int
        Host            byte[] UTF8 string
        Port            int
        MaxNumBytes     long

- GetStats            (6)

    Any node will respond

- GetObject            (7)

    Data nodes only

    Request Layout:

        MessageType       byte (7)
        KeyLen            int
        Key               byte[] UTF8 string

- PutObject            (8)

    Data nodes only.

    Request Layout:

        MessageType     byte (8)
        DataLen         int
        KeyLen          int
        Key             byte[] UTF8 string
        DataLen         int
        Data            byte[]

- DeleteObject         (9)

    Data nodes only.

    Request layout:

        MessageType    byte (9)
        KeyLen         int
        Key            byte[] UTF8 string

- ChangeConfig         (10)
     ?

### Responses

- InvalidRequestType     (1)

    Unrecognized initial byte

- NotMasterNode         (2) 
    
    A master request was sent to a data node

- NotDataNode             (3)

    A data node request was sent to the master

- ObjectOk                 (4) 

    Success response for GetObject

    Response layout:

    Data        byte[]

- ObjectMissing         (5)

    Data node owns that key but doesn't have the object
    
- ReConfigure             (6)

    Requested an object this data node doesn't have, client configuration is stale

    (Reponse layout same as *Configuration* below)

- Configuration         (7)

    Response layout:

        MessageType        byte (7)
        DataLen            int
            NumNodes            int
            [
                HostLen             int
                Host                byte[] UTF8 string
                Port                int
                MaxNumBytes         int
                NumLocations        int
                [Locations]         ints
            ]

## Security

For now the server is designed to run on a trusted network with well-behaved clients.  I wouldn't run a listener on a public IP.

# Coding Standards

Keep it neat.  Write good comments.  Provide csdoc comments.

Use spaces instead of tabs, 4 spaces per stop.  

Keep lines at 100 characters or less.

# Design Notes

Don't trust anything from here on down, they're just notes.

## Master API:

- GetConfig (Hash Ring, location of all data nodes and ring positions)
- NodeDown (report from client, not 100% trustworthy)
- AddNode
- RemoveNode
- ChangeNode
- GetStats
- Register

## Data Node API:

- GetObject 
    - I have it
    - I don’t have it but I should
    - I’m not responsible for this key.  Get config from master.
        - If I now think I’m responsible, start over.
        - If I’m still not responsible, send the new config to the client.
-PutObject 
    - I’m responsible, Ok
    - Do I have room for this object?  If yes, Ok
    - If no, push the oldest objects out until there is room.
        - I’m not responsible.  Get config from master.
        - If I’m now responsible, Ok
        - If I’m still not responsible, send the new config to the client.
- DeleteObject
    - I have it.  Ok.    
    - I don’t have it but should.  No-op.
    - I shouldn’t have it.  Get config from master and send to client.
- ChangeConfig (push from master)
    - Start migrating  (pushing) objects that shouldn’t be here
- GetStats

##    Node Configuration 

Stored on master, queried from nodes and clients on startup

- Host Name
- Port Number
- Max RAM Usage
- Ring Locations
- Status: Up, Down, Questionable


