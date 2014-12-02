Hadoop + Redis

In-Node Combiner
=======

Uses local Redis instance as a node-wise cache for mappers.
Mappers initially outputs results into the cache and defer emission to the last mapper in the node.
