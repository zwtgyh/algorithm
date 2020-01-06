#!bin/bash
today=$(date "+%Y-%m-%d")
today_1=$(date +"%Y-%m-%d" -d "-1 days")
today_2=$(date +"%Y-%m-%d" -d "-2 days")
dir_train=hdfs://nameservice1/home/dc/warehouse/model_sample/train/
dir_test=hdfs://nameservice1/home/dc/warehouse/model_sample/test/
today_2_2=`date -d "$today_2" +%s`
dir_train_today=${dir_train}stat_date=${today}
dir_train_today_1=${dir_train}stat_date=${today_1}
dir_test_today=${dir_test}stat_date=${today}
dir_test_today_1=${dir_test}stat_date=${today_1}
mvfunc(){
		for file in `hadoop fs -ls $1`
		do 
		    if [ "${file:0:4}"x = "hdfs"x ]; then
			    date_file=${file##*=}
			    date_file_2=`date -d "$date_file" +%s`
			    dir_file=$1stat_date=${date_file}
				    if [ $today_2_2 -gt $date_file_2 ]; then
				      hadoop fs -rm -r $dir_file
				      echo "deleted ${dir_file}"
				    fi
			fi
		done
}
ifexistfunc(){
	hadoop fs -test -e $1
	if [ $? -eq 0 ]; then
		hadoop fs -test -e $2
		if [ $? -eq 0 ]; then
			return 99
	    fi
	fi
}
ifexistfunc ${dir_train_today} ${dir_test_today}
ret_1=$?
ifexistfunc ${dir_train_today_1} ${dir_test_today_1}
ret_2=$?
if [ ${ret_1} -eq 99 ] || [ ${ret_2} -eq 99 ]; then
	mvfunc ${dir_train}
	mvfunc ${dir_test}
else 
   echo "last 2 days smaple dir is empty" 
fi