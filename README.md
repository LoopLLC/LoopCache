LoopCache
=========

A distributed key-value store, similar to memcached, written in C#

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

# Design Notes

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

##	Node Configuration (stored on master, queried from nodes and clients on startup)

- Host Name
- Port Number
- Max RAM Usage
- Ring Locations
- Status: Up, Down, Questionable

