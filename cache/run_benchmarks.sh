# GET_FUNCTION in {read_keys_no_cache, read_keys_with_cache}
# GEN_EXPERIMENT in {gen_fixed_experiment, gen_rand_experiment, gen_clustered_rand_experiment}
# 2^20 = 1MB = 1048576

# GET_FUNCTION= str
DATA_SIZE=1000000 # int
NUM_KEYS=320 # int
# NUM_INSTANCES= int
# NUM_CORES_PER_INSTANCE= int
NUM_KEYS_PER_CORE=10 # int
# GEN_EXPERIMENT= # str
# LAUNCH_GROUP= # bool
# TERMINATE_AFTER= # bool


declare -a experiments=("gen_fixed_experiment" "gen_rand_experiment" "gen_clustered_rand_experiment")
declare -a run_func=("read_keys_no_cache" "read_keys_with_cache")
declare -a num_cores=("1" "2" "4" "8" "32")
declare -a num_instances=("128" "64" "32" "16" "4")

for index in ${!num_cores[*]}; do
    n_ci=${num_cores[$index]}
    n_ii=${num_instances[$index]}
    launch_group=True
    terminate_after=False
    total_to_launch=6 # len(experiments) * len(run_func)
    for exi in "${experiments[@]}"
    do
        for r_fi in "${run_func[@]}"
        do
            #echo $r_fi $DATA_SIZE $NUM_KEYS $n_ii $n_ci $NUM_KEYS_PER_CORE $exi $launch_group $terminate_after
            python3 broadcast_benchmark_script.py $r_fi $DATA_SIZE $NUM_KEYS $n_ii $n_ci $NUM_KEYS_PER_CORE $exi $launch_group $terminate_after
            if [ "$launch_group" == "True" ]; then
                launch_group=False
            fi
            total_to_launch="$(($total_to_launch-1))"
            if [ "$total_to_launch" == "0" ]; then
                #echo $total_to_launch
                pywren standalone terminate_instances
            fi

        done
    done
done
