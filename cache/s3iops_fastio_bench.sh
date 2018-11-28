#/bin/bash

aws s3 rm s3://uric-cache-benchmarks --recursive --include "*" --exclude "test*" > /dev/null
pywren standalone launch_instances 25 --spot_price 0.0 --parallelism 36
python3 s3iops_cpp.py 1e6 1000

for data_size in 1e4 1e5 1e6 1e7 1e8
do
	for num_keys in 100 250 500 750 1000
	do
		python3 s3iops_cpp.py $data_size $num_keys
	done
done
pywren standalone terminate_instances
aws s3 rm s3://uric-cache-benchmarks --recursive --include "*" --exclude "test*" > /dev/null
