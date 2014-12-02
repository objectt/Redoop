Redoop = Hadoop + Redis
=======


In-Node Combiner
=======

Use a local Redis instance as a node-wise cache for mappers.
Mappers initially output results into the local cache and defer emission to the last mapper in the node.
