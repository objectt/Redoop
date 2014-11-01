#! /bin/bash



R1_HOST=147.46.121.177
R2_HOST=147.46.121.158
PORTS=( 7000 7001 7002 )

total=0

for port in "${PORTS[@]}"
do
	dbsize=$(redis-cli -h 147.46.121.158 -p $port dbsize)
	echo "147.46.121:158:$port - $dbsize"
	total=$(($dbsize+$total))

        dbsize=$(redis-cli -h 147.46.121.177 -p $port dbsize)
        echo "147.46.121.177:$port - $dbsize"
        total=$(($dbsize+$total))
done

echo "$total"
