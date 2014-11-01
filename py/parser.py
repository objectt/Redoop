#import redis
from rediscluster import RedisCluster

file_name = 'member.txt'

#r_server = redis.Redis("localhost", 7000)
#r_server = r_server.pipeline()

startup_nodes = [{"host": "127.0.0.1", "port": "7000"}]
r_server = RedisCluster(startup_nodes=startup_nodes)

with open(file_name, 'r') as file:
        data = file.readlines()

	#r_server.select(1)
	r_server.set("member_counter", 0)

        for i, line in enumerate(data):
                col = line.split(",")
                household_id = col[0]
                gender = col[3]
                age = col[8]
                education = col[5]
                occupation = col[6]
                married = col[7][0]

		#r_server.sadd("household", household_id)
		#print 'Householde #' + household_id + ' added'
		
		member_id = r_server.incr("member_counter")
		member_dict = {
			'gender' : gender,
			'age' : age,
			'education' : education,
			'occupation' : occupation,
			'married' : married
		}
		
		r_server.hmset("member:%d" % member_id, member_dict)
		#r_server.sadd("household:%s:member" % household_id, member_id)

                if i % 1000000 == 0:
			print i
                        #break
		
		#if i == 2000000:
		#	break

	print "i = " + i
        file.close()
	#r_server.execute()
