#/bin/bash

for method in async threaded
do
	for data_size in 1e4 1e5 1e6
	do
		for num_keys in 100 500 1000
		do
			python3 s3iops_lambda.py $data_size $num_keys $method
			sleep 5
		done
	done
done
