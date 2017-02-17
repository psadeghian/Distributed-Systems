This system aims to act as a distributed file system. 
The namenode keeps track of the directory setup and the location and size of the files. 
The files themselves are distributed to the datanodes.
The namenode distributes the files based on an algorithm. The algorithm picks the the datanode with the most available space as the host for the file being written onto the system. 
