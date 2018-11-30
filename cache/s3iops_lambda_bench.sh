#/bin/bash

for method in async threaded
do
	for data_size in 1e4 5e4 1e5 5e5 1e6 5e6 1e7 5e7 1e8
	do
		for num_keys in 100 250 500 750 1000
		do
			python3 s3iops_lambda.py $data_size $num_keys $method
			sleep 5
		done
	done
done
