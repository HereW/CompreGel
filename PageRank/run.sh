source ~/.bashrc
rm run
make run

# iter=(10 15 20 25 30 35)
iter=(20)
# sz_error="1e-9"

for i in ${iter[@]}
do
    LD_PRELOAD=../SZ3/lib64/libzstd.so mpiexec -n 32 -f ./conf ./run $i
    # LD_PRELOAD=../SZ3/lib64/libzstd.so mpiexec -n 32 -f ./conf ./run $i $sz_error

    hadoop_file=/toyOutput # requires to be consistent with the second parameter in function pregel_ppr in run.cpp
    folder=./result_toy
    file_2edit=$folder/result_toy_temp
    
    mkdir $folder
    temp1=sort_$(basename $file_2edit)
    file1=$folder/$temp1
    hadoop fs -cat $hadoop_file/* > $file_2edit
    sort -n -k 2 $file_2edit > $file1
    
    tail -n 10 $file1
done