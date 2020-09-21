#!/bin/bash

error() {
        echo "Usage:"
        echo "tabledata_verify_report.sh [-f config file] [-t new table name] [-o old table name] [-r max rate]"
        echo "config file: 配置文件路径，设置了此项后以下参数都会被忽略，所有信息将从配置文件中获取"
        echo "new table name: 需要统计的目标表名（新系统），与配置文件参数二选一使用"
        echo "old table name: 需要统计的目标表名（老系统），与配置文件参数二选一使用，不设置则默认使用新系统表名"
	echo "max rate: 新老系统最大允许的误差率"
	exit 1
}

while getopts "f:t:o:r:" opts
do
        case $opts in
                f) config_file="$OPTARG";;
                t) new_target_table="$OPTARG";;
                o) old_target_table="$OPTARG";;
		        r) max_rate="$OPTARG";;
                ?) error;;
        esac
done

if [[ -z $config_file ]] && [[ -z $new_target_table ]]
then
    echo "[config file] or [target table] must be set!"
    error
fi

if [ -z $old_target_table ]
then
	old_target_table=$new_target_table
fi

# 环境配置 #用来报警提示用的，测试环境可注销
. /app/toolbox/warehouse/common.sh
today=`date +"%Y%m%d"`
# today=`date -d "1 days ago" +"%Y%m%d"`
sp="###"
pdir=/tmp/tabledata_verify
dir=$pdir/$today
mkdir -p $dir
# 汇总结果（csv格式，对外输出）
result_csv=$pdir/result_$today.csv
rm -rf $result_csv
rm -rf $result_csv.total
err_log=$dir/err.log
rm -rf $err_log.*

# ftp配置
im=`whoami`
if [ $im == "root" ]
then
    ftp_export_path=/app/mtdp/hive/alljobs/export.sh
    alerter_log=/app/mtdp/mtdp/alert/mt_alerter.log
else
    ftp_export_path=~/alljobs/export.sh
    alerter_log=/app/$sysuser/mtdp/alert/mt_alerter.log
fi
. $ftp_export_path

download() {
	echo "download $old_report,$new_report from ftp server:/tmp/tabledata_statistic/$today"
    ftp -n <<EOF
 open ${remote_ip}
 user ${remote_user} ${remote_password}
 binary
 cd /tmp/tabledata_statistic/$today
 get $old_report $dir/$old_report
 get $new_report $dir/$new_report
 bye
EOF
}

wait_time=60
max_wait_cnt=60
ftp_file=/tmp/tabledata_verify/ftp.log
wait4() {
	wait_file1="$1"
	wait_file2="$2"
	is_ready=false
	wait_cnt=0
	while [ $is_ready == false ]
	do
		let wait_cnt=$wait_cnt+1
		exec 6>&1 1>$ftp_file
        	ftp -n <<EOF
 open ${remote_ip}
 user ${remote_user} ${remote_password}
 binary
 cd /tmp/tabledata_statistic/$today
 ls $wait_file1
 ls $wait_file2
 bye
EOF

	        exec 1>&6
	        exec 6>&-
	
	        cnt1=`cat $ftp_file | grep $wait_file1 | wc -l`
	        cnt2=`cat $ftp_file | grep $wait_file2 | wc -l`
	        if [[ $cnt1 -ge 1 ]] && [[ $cnt2 -ge 1 ]]
	        then
	                is_ready=true
	        else
			if [ $wait_cnt -ge $max_wait_cnt ]
			then
				echo "ftp file:$wait_file1,$wait_file2 doesn't exists after wait for $max_wait_cnt times, it may be wrong"
				exit 1
			else
				echo "ftp file:$wait_file1,$wait_file2 doesn't ready,wait for $wait_time s"
	                	sleep $wait_time
			fi
	        fi
	done
}

abs() {
	echo ${1-}
}

num_value_check() {
	new_value=$1
	old_value=$2
	d=$3
	col=$4
	if [ `echo "$old_value==$new_value" | bc` -eq 0 ]
	then
		if [[ $new_value -eq 0 ]] && [[ ! $old_value -eq 0 ]]
		then
			abs_diff_rate=100
		else
        		diff_rate=`echo "scale=4;($new_value-$old_value)/$new_value*100" | bc`
			abs_diff_rate=`abs $diff_rate`
		fi
        	if [ `echo "$abs_diff_rate>=$max_rate" | bc` -eq 1 ]
        	then
			echo "$d$sp$col$sp$old_value$sp$new_value$sp$abs_diff_rate" >> $result_sout
        	fi
	fi
}

str_value_check() {
	new_value=$1
	old_value=$2
	d=$3
	col=$4
	if [ "$new_value" != "$old_value" ]
	then
		echo "$d$sp$col$sp$old_value$sp$new_value$sp"100 >> $result_sout
	fi
}

verify() {
	if [ -z $max_rate ]
	then
		max_rate=0.01
	fi
	# 统计报告
	old_report=${old_target_table}.report.${today}.old
	new_report=${new_target_table}.report.${today}.new
	# 比对结果（列式维度，历史记录留存）
	result_sout=$dir/sout.${new_target_table}.log
	rm -rf $result_sout

	download
	if [[ ! -f $dir/$new_report ]] || [[ ! -f $dir/$old_report ]]
	then
		report=`cat $dir/$new_report | grep total | awk -F '###' '{print $2}'`
		if [ ! -f $dir/$new_report ]
		then
			echo "unknow new table:$new_target_table,report:$new_report" | tee -a $err_log.unknow
		elif [ -z "$report" ]
		then
			echo "unknow new table:$new_target_table,report:$new_report" | tee -a $err_log.unknow
		fi
		if [ ! -f $dir/$old_report ]
		then
			echo "unknow old table:$old_target_table,report:$old_report" | tee -a $err_log.unknow
		fi
		continue
	fi
	# total比对
	old_total=`cat $dir/$old_report | grep '^total#' | awk -F '###' '{print $NF}'`
	new_total=`cat $dir/$new_report | grep '^total#' | awk -F '###' '{print $NF}'`
	num_value_check $new_total $old_total "total_count" "all"
	# distinct比对
	distinct_cols=`cat $dir/$new_report | grep 'distinct@' | awk -F '@' '{print $NF}' | awk -F '###' '{print $1}'`
	if [ `cat $dir/$new_report | grep 'distinct@' | wc -l` -ge 1 ]
	then
		for col in ${distinct_cols[*]}
		do
			new_value=`cat $dir/$new_report | grep 'distinct@' | grep -i "@$col#" | awk -F '###' '{print $NF}'`
			old_value=`cat $dir/$old_report | grep 'distinct@' | grep -i "@$col#" | awk -F '###' '{print $NF}'`
			# 旧表中不存在该字段
			if [ -z $old_value ]
			then
				old_value=0
			fi
			num_value_check $new_value $old_value "distinct_count" $col
		done
	fi
	# number比对
	number_cols=`cat $dir/$new_report | grep 'number@' | awk -F '@' '{print $NF}' | awk -F '###' '{print $1}' | sort | uniq`
	if [ `cat $dir/$new_report | grep 'number@' | wc -l` -ge 1 ]
	then
		for col in ${number_cols[*]}
		do
			new_max_value=`cat $dir/$new_report | grep 'number@' | grep -i "@$col#" | grep max | awk -F '###' '{print $NF}'`
			old_max_value=`cat $dir/$old_report | grep 'number@' | grep -i "@$col#" | grep max | awk -F '###' '{print $NF}'`
			num_value_check $new_max_value $old_max_value "number_max" $col
			new_min_value=`cat $dir/$new_report | grep 'number@' | grep -i "@$col#" | grep min | awk -F '###' '{print $NF}'`
			old_min_value=`cat $dir/$old_report | grep 'number@' | grep -i "@$col#" | grep min | awk -F '###' '{print $NF}'`
			num_value_check $new_min_value $old_min_value "number_min" $col
			new_avg_value=`cat $dir/$new_report | grep 'number@' | grep -i "@$col#" | grep avg | awk -F '###' '{print $NF}'`
			old_avg_value=`cat $dir/$old_report | grep 'number@' | grep -i "@$col#" | grep avg | awk -F '###' '{print $NF}'`
			num_value_check $new_avg_value $old_avg_value "number_avg" $col
			new_std_value=`cat $dir/$new_report | grep 'number@' | grep -i "@$col#" | grep std | awk -F '###' '{print $NF}'`
			old_std_value=`cat $dir/$old_report | grep 'number@' | grep -i "@$col#" | grep std | awk -F '###' '{print $NF}'`
			num_value_check $new_std_value $old_std_value "number_std" $col
		done
	fi
	# enum比对
	enum_cols=`cat $dir/$new_report | grep 'enum@' | awk -F '@' '{print $NF}' | awk -F '###' '{print $1}'`
	if [ `cat $dir/$new_report | grep 'enum@' | wc -l` -ge 1 ]
	then
		for col in ${enum_cols[*]}
		do
			new_value=`cat $dir/$new_report | grep 'enum@' | grep -i "@$col#" | awk -F '###' '{print $NF}'`
			old_value=`cat $dir/$old_report | grep 'enum@' | grep -i "@$col#" | awk -F '###' '{print $NF}'`
			str_value_check $new_value $old_value "enum" $col
		done
	fi
	# section比对
	# sample比对
	if [ `cat $dir/$new_report | grep 'sample#' | wc -l` -ge 1 ]
	then
		new_sample_md5=`cat $dir/$new_report | grep 'sample#' | awk -F '###' '{print $NF}'`
		old_sample_md5=`cat $dir/$old_report | grep 'sample#' | awk -F '###' '{print $NF}'`
		str_value_check $new_sample_md5 $old_sample_md5 "sample_md5" "sample"
	fi

	# 有不一致
	is_check_detail=`cat $result_sout | wc -l`
	if [ $is_check_detail -ge 1 ]
	then
		echo "!!!!!found error!!!!!"
		let err_cnt=$err_cnt+1
		# 新表名,旧表名,异常列名,新表值,旧表值,误差率
		if [ `cat $result_sout | grep 'total_count#' | wc -l` -ge 1 ]
		then
			tnewv=`cat $result_sout | grep 'total_count#' | awk -F '###' '{print $4}'`
			toldv=`cat $result_sout | grep 'total_count#' | awk -F '###' '{print $3}'`
			trate_tmp=`cat $result_sout | grep 'total_count#' | awk -F '###' '{print $NF}'`
			trate=$(printf "%.2f" `echo "scale=2;$rate_tmp*1" | bc`)
			echo "$new_target_table,$old_target_table,$tnewv,$toldv,$trate%" >> $result_csv.total
		fi
		# 新表名,旧表名,异常列名,新表值,旧表值,误差率
		for col in ${distinct_cols[*]}
		do
			tmp_msg=`cat $result_sout | grep 'distinct_count#' | grep -i "#$col#" | awk -F '###' '{printf "%s!=%s\n",$4,$3}'`
			if [ ! -z "$tmp_msg" ]
			then
				newv=`cat $result_sout | grep 'distinct_count#' | grep -i "#$col#" | awk -F '###' '{print $4}'`
				oldv=`cat $result_sout | grep 'distinct_count#' | grep -i "#$col#" | awk -F '###' '{print $3}'`
				rate_tmp=`cat $result_sout | grep 'distinct_count#' | grep -i "#$col#" | awk -F '###' '{print $NF}'`
				rate=$(printf "%.2f" `echo "scale=2;$rate_tmp*1" | bc`)	
				echo "$new_target_table,$old_target_table,$col,$newv,$oldv,$rate%" >> $result_csv
			fi
		done
		for col in ${number_cols[*]}
		do
			max_msg=`cat $result_sout | grep 'number_max' | grep -i "#$col#" | awk -F '###' '{printf "%s!=%s\n",$4,$3}'`
			min_msg=`cat $result_sout | grep 'number_min' | grep -i "#$col#" | awk -F '###' '{printf "%s!=%s\n",$4,$3}'`
			avg_msg=`cat $result_sout | grep 'number_avg' | grep -i "#$col#" | awk -F '###' '{printf "%s!=%s\n",$4,$3}'`
			std_msg=`cat $result_sout | grep 'number_std' | grep -i "#$col#" | awk -F '###' '{printf "%s!=%s\n",$4,$3}'`
		done
		for col in ${enum_cols[*]}
		do
			tmp_msg=`cat $result_sout | grep 'enum#' | grep -i "#$col#" | awk -F '###' '{printf "%s!=%s\n",$4,$3}'`
		done
		if [ `cat $result_sout | grep 'sample#' | wc -l` -ge 1 ]
		then
			sample_msg="抽样数据md5值不一致"
		fi
	else
		# 没有不一致
		echo "#####no error#####"
		let noerr_cnt=$noerr_cnt+1
		touch $result_sout
	fi
}

get_result_csv() {
	# 标数量、误差率阈值、昨日告警表数量、误差率分布统计
	err_table_cnt=`cat $result_csv | grep -v '误差率' |awk -F , '{print $1}' | sort | uniq | wc -l`
	err_col_cnt=`cat $result_csv | grep -v '误差率' |awk -F , '{print $3}' | wc -l`
	. /app/toolbox/warehouse/common.sh
	new_cnt=`ls -al $dir | grep new | wc -l`
	old_cnt=`ls -al $dir | grep old | wc -l`
	# 多个配置文件一起跑的情况下，需要区分哪些表属于当前配置文件
	sout_tables=`ls -al $dir | grep sout |awk '{print $NF}' | awk -F '.' '{printf "%s.%s\n",$(NF-2),$(NF-1)}'`
	sout_cnt=0
	for i in ${sout_tables[*]}
	do
		cnt=`cat $config_file | grep $i | wc -l`
		if [ $cnt -ge 1 ]
		then
			let sout_cnt=$sout_cnt+1
		fi
	done
	# sout_cnt=`ls -al $dir | grep sout | wc -l`
	cp $result_csv $result_csv.tmp
	# 数据入库hive
	# create external table tmp.tabledata_verity_result(partition_field string,new_table string,old_table string,err_col string,new_value string,old_value string) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile location '/data/tmp.db/tabledata_verity_result'
	hdfs_path=/data/tmp.db/tabledata_verity_result
	pk_day=`date +"%Y-%m-%d"`
	cat $result_csv.tmp | awk -v pkd="$pk_day" '{printf "%s,%s\n",pkd,$0}' > $result_csv.hdfs
	hadoop fs -rmr $hdfs_path/result_${today}.csv.hdfs
	hadoop fs -put $result_csv.hdfs $hdfs_path
	rm -rf $result_csv.hdfs
	echo "upload data to table: tmp.tabledata_verity_result"
	# 统计csv结果报告
	total_table_cnt=`cat $co/nfig_file | sort | uniq |wc -l`
	total_cnt=$total_table_cnt
	err_rate=`echo "scale=2;$err_cnt/$total_cnt*100" | bc`
	echo "总表数,已核查表,正常表,异常表,异常率,异常字段总数,误差率阈值" > $result_csv
	echo "$total_cnt,$sout_cnt,$noerr_cnt,$err_cnt,$err_rate%,$err_col_cnt,$max_rate%" >>  $result_csv
	echo "" >> $result_csv
	echo "表名或者字段不存在异常表" >> $result_csv
	cat $err_log.unknow >> $result_csv
	echo "" >> $result_csv
	echo "表级异常概览"
	echo "新表名,旧表名,新表总数,旧表总数,总数误差率,异常字段列表" >> $result_csv
	# 根据表名聚合字段
	cat $result_csv.tmp | awk -F , '{arr[$1","$2]=arr[$1","$2]","$3}END{for(i in arr){printf "%s%s\n",i,arr[i]}}' >> $result_csv.tmp1
	# 根据表名匹配该表的总数量误差结果
	for i in `cat $result_csv.tmp1`/
	do
		key=`echo $i | awk -F , '{printf "%s,%s\n",$1,$2}'`
		sub=`echo $i | awk -F , '{for(i=3;i<=NF;i++){printf ",%s",$i}}'`
		total_info=`cat $result_csv.total | grep $key | awk -F , '{printf "%s,%s,%s\n",$3,$4,$5}'`
		if [ -z "$total_info" ]
		then
			total_info="-,-,无误差"
		fi
		add_time_key=`echo $i | awk -F, '{printf "%s#%s\n",$1,$2}'`
		add_time=`cat $config_file | grep $add_time_key | awk -F '#' '{print $12}'`
		echo $key,$add_time,$total_info$sub >> $result_csv.tmp2
	done
	# 不在异常字段表中的总数量误差表补充
	for i in `cat $result_csv.total` 
	do
		key=`echo $i | awk -F , '{printf "%s,%s\n",$1,$2}'`
		cnt=`cat $result_csv.tmp2 | grep $key | wc -l`
		if [ $cnt -lt 1 ]
		then
			echo "$i" >> $result_csv.tmp2
		fi
	done
	cat $result_csv.tmp2 | sort -t , -k 5 -nr >> $result_csv
	
	echo "" >> $result_csv
	echo "字段差异详情" >> $result_csv
	echo "新表名,旧表名,异常字段名,新系统去重值,旧系统去重值,误差率" >> $result_csv
	cat $result_csv.tmp | sort -t , -k 6 -nr >> $result_csv
	rm -rf $result_csv.tmp1
	rm -rf $result_csv.tmp2
	rm -rf $result_csv.tmp
	rm -rf $result_csv.total
	# 告警
	unknow_new_cnt=`cat $err_log.unknow | grep new | wc -l`
	unknow_old_cnt=`cat $err_log.unknow | grep old | wc -l`
	alert_msg=$alert_title"_大数据平台#新老系统数据校验#SUCCESS#总表数:$total_cnt,已核查表数:$sout_cnt,正常表$noerr_cnt个,异常表$err_cnt个,异常率$err_rate%,异常字段共$err_col_cnt个,比对误差率阈值为$max_rate%,异常表数:$unknow_new_cnt+$unknow_old_cnt,比对统计报告路径:$result_csv" 
	echo $alert_msg | tee -a $alerter_log
}

if [ ! -z $config_file ]
then
	if [ ! -f $config_file ]
	then
		echo "config_file:$config_file doesn't exists!"
		exit 1
	fi
	err_cnt=0
	noerr_cnt=0
	# 配置文件结构：new_table#old_table#pk_field#pk_date#out_fields#distinct#number#enum#sample#rate#is_run
	conf_name=`echo $config_file | awk -F '/' '{print $NF}'`
	new_ok_file=${today}_new_${conf_name}.ok
	old_ok_file=${today}_old_${conf_name}.ok
	wait4 $new_ok_file $old_ok_file
	cnt=`cat $config_file | wc -l`
	i=0
	for conf in `cat $config_file`
	do
		let i=$i+1
        new_target_table=`echo $conf | awk -F '#' '{print $1}'`
        old_target_table=`echo $conf | awk -F '#' '{print $2}'`
        #max_rate=`echo $conf | awk -F '#' '{print $(NF-1)}'`
        #is_run=`echo $conf | awk -F '#' '{print $NF}'`
		max_rate=`echo $conf | awk -F '#' '{print $10}'`
		is_run=`echo $conf | awk -F '#' '{print $11}'`
		add_time=`echo $conf | awk -F '#' '{print $12}'`
		if [[ -z "$new_target_table" ]] || [[ -z "$old_target_table" ]]
		then
			echo "target table error,new_target_table:$new_target_table,old_target_table:$old_target_table"
			continue
		fi
		if [ "$is_run" == "n" ]
                then
                        echo "$new_target_table,$old_target_table has been excepted!"
                        continue
                fi
		verify
	done
	get_result_csv
else
	verify
fi
