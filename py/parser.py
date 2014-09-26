import redis

file_name = 'nielsen.db/member/Member20100701_20130731.txt'

r_server = redis.Redis("localhost")

with open(file_name, 'r') as file:
        data = file.readlines()

        for i, line in enumerate(data):
                col = line.split(",")
                household_id = col[0]
                gender = col[3]
                age = col[4]
                education = col[5]
                occupation = col[6]
                married = col[7][0]

                print married

                if i == 10:
                        break

        file.close()
