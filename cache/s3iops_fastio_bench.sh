#/bin/bash

pywren standalone launch_instances 25 --spot_price 0.0 --parallelism 36 --max_idle_time 99999999
python3 s3iops_cpp.py 1e6 1000
sleep 5

for data_size in 1e4 1e5 1e6 1e7
do
	for num_keys in 100 250 500 750 1000
	do
		python3 s3iops_cpp.py $data_size $num_keys
		sleep 5
		python3 s3iops.py $data_size $num_keys async
		sleep 5
	done
done
pywren standalone terminate_instances
