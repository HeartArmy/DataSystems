# you need to install pymemcache
import random, faker
from pymemcache.client.base import Client
from pymemcache.client.murmur3 import murmur3_32

# stub
def add_node(name, ip, port):
	### m1 = Client(('localhost', 11211))
	return

# stub
def remove_node(name):
	### m1.close()
	return

# stub
def dht_get(key):
	return

# stub
def dht_set(key, value):
	### note: replication factor is 2
	return

# Helper function
def read_list_func(rlist):
	# Read the values of the sampled keys from your DHT
	for key in rlist:
	    value = dht_get(str(key))
	    print(f"Key: {key}, Value: {value}")


def main():
    print("My Memcached DHT.")
	# Add Memcached Instances
	print("Initializing the DHT Cluster:")
	add_node('m1','localhost', 11211)
	add_node('m2','localhost', 11212)
	add_node('m3','localhost', 11213)
	add_node('m4','localhost', 11214)

	# Write 100 key-value pairs to Memcached (you add more if you want)
	print("Loading some fake data")
	fake = faker.Faker()
	for key in range(100):
	    value = fake.company_name()
	    key_hash = murmur3_32(str(key))
    	dht_set(str(key_hash), value)

	### TEST
	# Sample 10 random keys to read from Memcached to test the system
	read_list = random.sample(range(100), 10)
	print("My Memcache DHT")

	# Check the status of the value
	read_list_func(read_list) # Check the content of the cache

	# Simulating the failure of a node m1
	# remove_node('m1')
	read_list_func(read_list) # Check the content of the cache

	# Simulating the addition of a new node m5
	add_node('m5','localhost', 11215)
	read_list_func(read_list) # Check the content of the cache

if __name__ == "__main__":
    main()