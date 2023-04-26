# you need to install pymemcache
import random, faker
from pymemcache.client.base import Client
from pymemcache.client.murmur3 import murmur3_32

# Define the Memcached instances and name for hashes
MEMCACHED_INSTANCES = [
    {'name': 'm1', 'ip': 'localhost', 'port': 11211},
    {'name': 'm2', 'ip': 'localhost', 'port': 11212},
    {'name': 'm3', 'ip': 'localhost', 'port': 11213},
    {'name': 'm4', 'ip': 'localhost', 'port': 11214},
	{'name': 'm5', 'ip': 'localhost', 'port': 11215}
]
INSTANCE_HASHES = [murmur3_32(instance['name']) for instance in MEMCACHED_INSTANCES]


def create_client(instance):
    try:
        return Client((instance['ip'], instance['port']))
    except pymemcache.exceptions.PymemcacheError:
        logging.error(f"Failed to connect to {instance['name']} at {instance['ip']}:{instance['port']}")
        return None

MEMCACHED_CLIENTS = [create_client(instance) for instance in MEMCACHED_INSTANCES]

# Helper function to find the primary and secondary nodes for a given key
def get_primary_and_secondary(key):

    key_hash = murmur3_32(str(key))
    for i in range(len(INSTANCE_HASHES)):
        if INSTANCE_HASHES[i] >= key_hash:
            primary_index = i
            secondary_index = (i + 1) % len(INSTANCE_HASHES)
            return MEMCACHED_CLIENTS[primary_index], MEMCACHED_CLIENTS[secondary_index]
    # If no instance hash is greater than or equal to the key hash, use the first instance
    return MEMCACHED_CLIENTS[0], MEMCACHED_CLIENTS[1]

# Function to add a new Memcached instance
def add_node(name, ip, port):
    instance = {'name': name, 'ip': ip, 'port': port}
    instance_hash = murmur3_32(name)
    client = create_client(instance)

    if client is None:
        return

    try:
        MEMCACHED_INSTANCES.append(instance)
        INSTANCE_HASHES.append(instance_hash)
        MEMCACHED_CLIENTS.append(client)
    except:
        # Roll back the modification
        MEMCACHED_INSTANCES.remove(instance)
        INSTANCE_HASHES.remove(instance_hash)
        MEMCACHED_CLIENTS.remove(client)

# Function to remove a Memcached instance
def remove_node(name):
    for i, instance in enumerate(MEMCACHED_INSTANCES):
        if instance['name'] == name:
            del MEMCACHED_INSTANCES[i]
            del INSTANCE_HASHES[i]
            MEMCACHED_CLIENTS[i].close()
            del MEMCACHED_CLIENTS[i]
            break

# Function to read a key from the DHT
def dht_get(key):

    primary, secondary = get_primary_and_secondary(key)
    try:
        value = primary.get(str(key)).decode() #if you dont want the b infront of the email adresses, since its in byte by default
    except pymemcache.exceptions.PymemcacheError:
        logging.error(f"Failed to read key {key} from {primary}")
        value = None

    if value is None:
        try:
            value = secondary.get(str(key))
        except pymemcache.exceptions.PymemcacheError:
            logging.error(f"Failed to read key {key} from {secondary}")
            value = None

    return value



# Function to write a key/value pair to the DHT 
def dht_set(key, value):


    primary, secondary = get_primary_and_secondary(key)
    primary_index = MEMCACHED_CLIENTS.index(primary)
    next_indices = [(primary_index + i) % len(MEMCACHED_CLIENTS) for i in range(1, 3)]
    nodes = [MEMCACHED_CLIENTS[primary_index]] + [MEMCACHED_CLIENTS[i] for i in next_indices]

    for node in nodes:
        node.set(str(key), value)
    print(f"Key: {key}, Value: {value} written to nodes {nodes}")


# Helper function to read the values of the sampled keys from the DHT
def read_list_func(rlist):
    for key in rlist:
        value = dht_get(str(key))
        print(f"Key: {key}, Value: {value} retrieved from cache")

# Function to remove a Memcached instance
def remove_node(name):
    for i, instance in enumerate(MEMCACHED_INSTANCES):
        if instance['name'] == name:
            del MEMCACHED_INSTANCES[i]
            del INSTANCE_HASHES[i]
            MEMCACHED_CLIENTS[i].close()
            del MEMCACHED_CLIENTS[i]
            break

def test_check_content(): #extra test function to get more detailed testing
    # Create 5 Memcached instances
    instances = [
        {'name': 'm1', 'ip': 'localhost', 'port': 11211},
        {'name': 'm2', 'ip': 'localhost', 'port': 11212},
        {'name': 'm3', 'ip': 'localhost', 'port': 11213},
        {'name': 'm4', 'ip': 'localhost', 'port': 11214},
        {'name': 'm5', 'ip': 'localhost', 'port': 11215}
    ]

    # Create the Memcached clients
    clients = [Client((instance['ip'], instance['port'])) for instance in instances]

    # Write some key-value pairs to Memcached
    fake = faker.Faker()
    for key in range(10):
        value = fake.company_email()
        clients[key % len(clients)].set(str(key), value) #every client gets different value randomly

    # Check the content of the cache for all instances
    for client in clients:
        print(f"Cache content for {client}")
        for key in range(10):
            value = client.get(str(key))
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
		# print (key)
		value = fake.company_email()
		key_hash = murmur3_32(str(key))
		# print (key_hash)
		dht_set(str(key_hash), value)

	### TEST
	# Sample 10 random keys to read from Memcached to test the system
	key_hashes = [murmur3_32(str(key)) for key in range(100)]
	read_list = random.sample(key_hashes, 10)


	print("My Memcache DHT")



    # Check the status of the value
	for i in range(3):
		result = read_list_func(read_list)
		if i == 0:
			prev_result = result
		elif prev_result == result:
			if i == 2:
				print("Code works successfully") #only if the content returned is the same
		else:
			print("Error: read_list_func returned different results")
			break

    # Simulating the failure of a node m1
	remove_node('m1')
	for i in range(3):
		result = read_list_func(read_list)
		if i == 0:
			prev_result = result
		elif prev_result == result:
			if i == 2:
				print("Code works successfully")
		else:
			print("Error: read_list_func returned different results")
			break

	# Simulating the addition of a new node m5
	add_node('m5','localhost', 11215)
	for i in range(3):
		result = read_list_func(read_list)
		if i == 0:
			prev_result = result
		elif prev_result == result:
			if i == 2:
				print("Code works successfully")
		else:
			print("Error: read_list_func returned different results") #automating test
			break

	#UNCOMMENT TEST FUNCTION TO SEE MORE DETAILED TEST
	# test_check_content() #addtional test function I wrote to manually check the content of key value in each memcached node





if __name__ == "__main__":
    main()