#/bin/bash

pywren standalone launch_instances 25 --spot_price 0.0 --parallelism 36 --max_idle_time 99999999
python3 s3iops.py 1e6 1000 async
sleep 5

for method in async threaded
do
	for data_size in 1e4 1e5 1e6
	do
		for num_keys in 100 500 1000
		do
			# python3 s3iops_cpp.py $data_size $num_keys
			# sleep 5
			python3 s3iops.py $data_size $num_keys $method
			sleep 5
		done
	done
done
pywren standalone terminate_instances

bash -x s3iops_lambda_bench.sh
