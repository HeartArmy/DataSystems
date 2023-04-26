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