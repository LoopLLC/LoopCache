# A simple Ruby client to help test Loop Cache
#
# This is a proof-of-concept to show that Loop Cache can be used from 
# any programming language, not just C#.

require_relative "client"

# These are hard coded keys and hashes that we need to be able to produce
h = {}
h["6afc9cd0-a312-495d-958e-3f5ee1021dc9"] = 207271529
h["58aad64e-781e-45ed-a516-e0466fdb421c"] = 793011885
h["b74e36e7-75f7-4e37-8137-2a7ebf09ea3a"] = -2092457456
h["73974cd7-7f82-4165-8d2b-756420b8ce7c"] = 1370574413
h["8fee3e72-7e2f-41ac-9e01-15786f462eda"] = 1781424005
h["f9e9f75c-14ed-43ce-a58d-64dd0e4cf47d"] = -1283683673
h["49249af7-8efa-49a6-bdb4-83f2c5f98557"] = -1108612196
h["6c2b873c-bc26-4891-81cf-6a2092a127df"] = -1697063316
h["25c43b37-8515-4e4a-847d-bd2468a9e359"] = -953497936
h["36ab9e2f-601e-4197-97fa-742b31da9ec9"] = -437896523
h["f39d1d95-af0f-4a28-8178-73f93c22096f"] = -2095074639

puts "Key\t\t\t\t\tC#\t\tRuby"

all_passed = true;

h.each do |key, theirs|
    ours = LoopCacheClient.get_consistent_hash key
    puts "#{key}\t#{theirs}\t#{ours}"
    if ours != theirs
        all_passed = false
    end
end

if all_passed
    puts "All tests passed"
else
    puts "Some tests failed"
end


