## Distributed File System
#### by Evan Steiner
#### August 29th 2019

### Some thoughts on the benefits of erasure coding vs replication schemas

Erasure coding and replication both represent reliable methods for implementing fault tolerance into a file system. 
They each have benefits and downsides.
	
Replication provides a speed advantage and network traffic advantage over erasure coding on file upload and retrieval. 
Specifically, the client need only upload 1 file and download 1 file. Although the chunk servers will replicate that one 
file into multiple (3 in our case), it still doesn’t require as many files to be sent by the client and written to disk. 
In the case of erasure coding significantly more files must be uploaded by the client (9 in our case) and written to disk 
by the chunk servers, resulting in a slower upload process and more network traffic. Furthermore, at least 6 of the 
9 (in our case) files must be retrieved by the client to reconstruct the original file. This takes significantly longer 
than retrieving a single file. Although there are some aspects of replication that require extra time, specifically the 
need to create checksums, it is not nearly enough to slow it down.
	
On the other hand, should a file actually get corrupted, replication can become slower and cause more network traffic 
than erasure coding. When a file is corrupted in a replication based scheme, the client must then retrieve the file 
from another server. Furthermore the chunk server must request a file repair from another chunk server that holds the 
same chunk of the file. Erasure coding doesn’t have this issue. No checksums are needed and if a few of the pieces are 
corrupt it doesn’t matter. This means the client will likely be able to retrieve the file faster.
	
One other important aspect to consider is the storage space required by both schema.
A replication scheme will require significantly more space than an erasure coding scheme. 
Although this likely isn’t a factor most of the time it could be an issue in some settings.
