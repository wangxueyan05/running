#!/bin/bash

# ./tabledata_statistic_report.sh -a new -t dwd2.dwd_rcs_market -c certno,loan_no -e txn_type -n txn_am,loan_cycle -i test.sql -s 200
# ./tabledata_statistic_report.sh -a new -f conf_file
 
error() {
        echo "Usage:"
        echo "tabledata_statistic_t.sh [-x exec engine] [-a system type] [-f config file] [-t table name] [-k partition filed] [-d partition date] [-o out fields] [-c distinct columns] [-e enum columns] [-n number columns] [-i case sql] [-s sample data] [-h headn]repor"
        echo "exec engine: 执行引擎，hive or db2，生成的sql有细微差别"
        echo "system type: 系统类型，new为新系统，old为老系统"
        echo "config file: 配置文件路径，设置了此项后以下参数都会被忽略，所有信息将从配置文件中获取"
        echo "table name: 需要统计的目标表名，与配置文件参数二选一使用"
        echo "distinct columns: 可选项，需要去重计数的核心列名，如四要素等，多个列名使用逗号隔开"
        echo "enum columns: 可选项，需要分组统计的枚举列名，逗号隔开"
        echo "number columns: 可选项，需要统计总和/最大值/最小值/平均值/标准差等信息的数值列名，逗号隔开"
        echo "case sql: 可选项，需要统计分区区间分布的sql文件，文件内容为case when语句，每个sql为一行"
        echo "sample data: 可选项，抽样比对的数据量"
        exit 1
}

while getopts "x:a:f:t:k:d:o:c:e:n:i:s:h:y:mz" opts
do
        case $opts in
                x) exec_engine="$OPTARG";;
                a) system_type="$OPTARG";;
                f) config_file="$OPTARG";;
                t) target_table="$OPTARG";;
                k) pk_field="$OPTARG";;
                d) pk_date=" G";;
                o) out_fields="$OPTARG";;
                c) distinct_cols="$OPTARG";;
                e) enum_cols="$OPTARG";;
                n) num_cols="$OPTARG";;
                i) case_sql_file=$OPTARG;;
                s) sample_num=$OPTARG;;
                h) headn=$OPTARG;;
                y) bank_name=$OPTARG;;
                m) is_max_compare=1;;
                z) is_follow_max_compare=1;;
                ?) error;;
        esac
done

if [ -z $exec_engine ]
then
        # 默认为hive
        exec_engine=hive
fi
echo "exec_engine:$exec_engine"

conn_db2() {
        echo "connect to db2"
        db2 connect to $db2name user $db2user using $db2pwd
        if [ $? -ne 0 ]
        then
                echo "connect to db2 error!"
                echo "db2 connect to $db2name user $db2user using $db2pwd"
                exit 1
        fi
}


if [ -z $system_type ]
then
    echo "[system type] must be set!"
    error
else
    if [[ $system_type != "new" ]] && [[ $system_type != "old" ]]
    then
        echo "[system type] must be new or old!"
        error
    fi
fi

if [[ -z $config_file ]] && [[ -z $target_table ]]
then
        echo "[config file] or [target table] must be set!"
        error
fi

# 没有配置检查参数的情况下采用默认统计策略
param_check() {
        default_check=false
        if [[ -z $distinct_cols ]] && [[ -z $enum_cols ]] && [[ -z $num_cols ]] && [[ -z $case_sql_file ]]
        then
                echo "no custom params found,use default check"
                default_check=true
        fi
}

# 环境配置
basedir=`cd $(dirname $0); pwd -P`
today=`date +"%Y%m%d"`
# today=`date -d "1 days ago" +"%Y%m%d"`
sp="###"
dir=/tmp/tabledata_statistic/$today
#读取配置文件中的最后一个域的字段 $config_file=config_tj/tables_list.config
conf_name=`echo $config_file | awk -F '/' '{print $NF}'`
ok_file=$dir/${today}_${system_type}_${conf_name}.ok
if [ ! -d $dir ]
then
        mkdir -p $dir
fi
err_log=$dir/err.log
exec_log=$dir/exec.log

# db2配置
if [ $exec_engine == "db2" ]
then
        config_path=`echo $config_file | awk -F "$conf_name" '{print $1}'`
        db2_config=${config_path}db2.config
        if [ ! -f $db2_config ]
        then
                echo "$db2_config doesn't exists!"
                error
        fi
        db2name=`cat $db2_config | grep 'db=' | awk -F '=' '{print $NF}'`
        db2user=`cat $db2_config | grep 'user=' | awk -F '=' '{print $NF}'`
        db2pwd=`cat $db2_config | grep 'pwd=' | awk -F '=' '{print $NF}'`
        conn_db2
else
        kerberosKeytabFilePath=/app/mtdp/functions/tj/tietladmin.keytad
        kerberosPrincipal=tjetladmin@PROD.AIC.COM 
        kinit -kt ${kerberosKeytabFilePath}  ${kerberosPrincipal}
fi

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
        filename=$1
        targetpath=$2
        echo "download $filename from ftp server:/tmp/tabledata_statistic_${bank_name}/$today"
       ftp -n <<EOF
       open ${remote_ip}
       user ${remote_user} ${remote_possword}
       binary
       cd /tmp/tabledata_statistic_${bank_name}/$today
       bye
EOF
}
wait_time=60
max_wait_cnt=600
ftp_file=/tmp/tabledata_statistic_${bank_name}/ftp.log
wait4() {
        wait_file1="$1"
        wait_file2="$2"
        is_ready=false
        wait_cnt=0
        while [ $is_ready == false ]
        do
                let wait_cnt=wait_cnt+1
                exec 6>&1 1>$ftp_file
                ftp -n <<EOF
        open ${remote_ip}
       user ${remote_user} ${remote_possword}
       binary
       cd /tmp/tabledata_statistic_${bank_name}/$today
       ls $wait_file1
       ls $wait_file2
       bye
EOF
                exec 1>&6
                exec 6>$-
                cnt1=`cat $ftp_file ｜ grep $wait_file1 | wc -l`
                cnt2=`cat $ftp_file |grep $wait_file2 | wc -l`
                if [[  $cnt1 -ge 1  ]] && [[  $cnt2 -ge 1 ]]
                then
                        is_ready=true
                else
                        if [ $wait_cnt  -ge $max_wait_cnt ]
                        then
                                echo "ftp file:$wait_file1,$wait_file2 doesn't exists after wait for $max_wait_cnt times , it may be wrong"
                                exit 1
                        else 
                                echo "ftp file:$wait_file1,$wait_file2 doesn't ready,wait for $wait_time s"
                                sleep $wait_time
                                fi
                fi
        done

}

delete() {
        dir=$1
        file=$2
        ftp -n <<EOF
         open ${remote_ip}
         user ${remote_user} ${remote_password}
         binary
         cd $dir
         del $file
         bye
EOF
        echo "delete $dir/$file"
}


upload() {
        upload_file=$1
        echo "upload $upload_file to ftp server:/tmp/tabledata_statistic/$today"
        ftp -n <<EOF
 open ${remote_ip}
 user ${remote_user} ${remote_password}
 binary
 mkdir /tmp/tabledata_statistic
 mkdir /tmp/tabledata_statistic/$today
 cd /tmp/tabledata_statistic/$today
 del $upload_file
 put $upload_file
 bye
EOF
}

add_partition() {
        if [[ $have_partition -ge 1 ]] && [[ exec_engine == hive ]]
        then
                if [[ -z $add_time_hive ]] 
                then
                        sql=$sql" where $pk_field='$partition_date'"
                else
                        sql=$sql" where $pk_field='$partition_date' and $add_time_hive >='partition_date 00:00:00' and $add_time_hive <= 'partition_date 23:59:59"
                
        elif [[ $have_partition -ge 1 ]] && [[ exec_engine == db2 ]]
        then
                if [[ -z $add_time_db2 ]] 
                then
                        sql=$sql" where $pk_field='$partition_date'"
                else
                        sql=$sql" where $pk_field='$partition_date' and $add_time_db2 >='partition_date 00:00:00' and $add_time_db2 <= 'partition_date 23:59:59"
                fi        

                if [ ! -z $where_sql ]
                then
                        sql=$sql" and $where_sql"
                fi           
        fi
}

get_part_sql() {
        part_sql=`echo $sql | awk -F ' from' '{print $1}' | awk -F 'select ' '{print $NF}'`
}

generate_sql() {
        dim=$1
        col=$2
        if [ $dim == "total" ]
        then
                sql="select count(1) from $target_table"
                add_partition
        elif [ $dim == "distinct" ]
        then
                get_part_sql
                if [ $exec_engine == "hive" ]
                then
                        sql="select $part_sql,count(distinct(if($col is null,'',$col))) from $target_table"
                else
                        sql="select $part_sql,count(distinct(coalesce(cast($col as varchar(1000)),''))) from $target_table"
                fi
                add_partition
        elif [ $dim == "num" ]
        then
                get_part_sql
                sql="select $part_sql,max($col),min($col),avg($col),std($col) from $target_table"
                add_partition
        elif [ $dim == "enum" ]
        then
                sql="select $col,count(1) from $target_table group by $col"
                if [ $have_partition -ge 1 ]
                then
                        tmp_sql=`echo $sql | awk -F 'group' '{print $1}'`
                        sql=$tmp_sql" where $pk_field='$partition_date' group by $col"
                fi
        elif [ $dim == "case_when" ]
        then
                sql="select $col from $target_table"
        elif [ $dim == "sample" ]
        then
                sql="select * from $target_table limit $sample_num"
                if [ $have_partition -ge 1 ]
                then
                        tmp_sql=`echo $sql | awk -F 'limit' '{print $1}'`
                        sql=$tmp_sql" where $pk_field='$partition_date'"
                fi
        else
                echo "dim:$dim error"
        fi
}

exec_sql() {
        echo $sql
        if [ $exec_engine == "hive" ]
        then
                #result=`hive -e "$sql;"`
                result=`hive --showheader=false -e "$sql;" | sed 's/-//g' | sed 's/+//g' | sed 's/|//g'`
        else
                result=`db2 -x "$sql"`
        fi
        # sql执行失败记录
        if [ $? -ne 0 ]
        then
		let err_cnt=$err_cnt+1
                echo "execute error,table:$target_table,sql:$sql,error:$result" | tee -a $err_log
                continue
        fi
}   


err_cnt=0
statistic() {
	if [ $exec_engine == "hive" ]
	then
		pk_field="partition_field"
	else
		if [ -z $pk_field ]
		then
			pk_field="partition_date"
		fi
	fi
        echo "pk_field:$pk_field"
	db2_tmp="db2_exec_info.$target_table" 
        if [ -z $pk_date ]
        then
                partition_date=`date -d "2 days ago" +"%Y-%m-%d"`
	else
                partition_date=`date -d "$pk_date days ago" +"%Y-%m-%d"`
                #partition_date=$pk_date
        fi
	echo partition_date:$partition_date
        #拼接日志路径
        sout_log=$dir/${target_table}.report.${today}.${system_type}
        start=$(date +%s)
        param_check
	if [ $pk_field == "null" ]
	then
		have_partition=0
	else
        	if [ $exec_engine == "hive" ]
        	then
                	have_partintion=`hive -e "desc $target_table" |grep -i $pk_field | wc -l`
        	else
                	db2 -x describe table $target_table > $db2_tmp
                	have_partition=`cat $db2_tmp | grep -i $pk_field | wc -l`
        	fi
	fi
	if [ $? -ne 0 ]
	then
		let err_cnt=$err_cnt+1
		echo "$target_table doesn't exists!" | tee -a $err_log
		exit 1
	fi
        if [[ ! -z "$is_max_compare"  ]] || [[  ! -z "$is_follow_max_compare"   ]]
        then
                let err_cnt=$err_cnt+1
                echo "$target_table doesn't exists!" | tee -a $err_log
                exit 1
        fi
        if [[ ! -z $is_max_compare ]] || [[ ! -z  "$is_follow_max_compare" ]]
        then
                have_partintion=0  
        fi
        echo "$target_table table's partition_key has $have_partition"
	
	#headn=20
        if [-z $headn]
        then
                headn=30
        fi
        if [ $default_check == true ]
        then
                if [ -z $out_fields ]
                then
                        grep_out=","
                else
                        grep_out=`echo $out_fields | sed 's/,/\\\|/g'`
                fi
                if [ $exec_engine == "hive" ]
                then
                        #all_cols=`hive -e "desc $target_table" | grep -v '#' |grep -v partition | grep -v date | grep -v etl |grep -v Fetched | grep -v $ | awk '{print $1}' | head -n $headn`
                        all_cols=`hive --showheader=false -e "desc $target_table" | grep -v '#' | awk '{printf "%s\n",$2}' | awk '{if($0!="*"&&$0!="|*|"){print $0}}' | grep -v '#'| grep -iv partition| grep -iv date |grep -v \|grep -iv et|grep -v Fetched | grep -v $grep_out | sort | head -n $headn `
                else
                	#db2 -x describe table $target_table > $db2_tmp
                        tabname=`echo $target_table | awk -F '.' '{print $2}'`
                        tabschema=`echo $target_table | awk -F '.' '{print $1}'`
                        db2 -x "select colname from stscat.columns where tabname='$tabname'and tabschnema='$tabschema'" > $db2_tmp
                        all_cols=`cat $db2_tmp | grep -v '#' | grep -v partition | grep -v date | grep -v etl |grep -v Fetched | grep -v $grep_out | awk '{print $1}' | head -n $headn`
                fi
                distinct_cols=`echo $all_cols | sed 's/ /,/g'`
                echo "default check:set distinct_cols to all_cols"
                echo "default check:set distinct_cols to all_cols"
                # num_cols=$distinct_cols
        fi
        # 统计数据总量
        generate_sql "total"
        if [ ! -z "$is_max_compare" ]
        then
                exec_sql
        else
                
                # 统计列去重数
                if [ ! -z $distinct_cols ]
                then
                        for col in `echo $num_cols| awk -F , '{for(i=1;i<=NF;i++){printf "%s\n",$i}}'`
                        do
                        generate_sql "num" $col
                        done
                fi
                # 统计数值类型
                if [ ! -z $num_cols ]
                then
                        for col in `echo $num_cols | awk -F , '{for(i=1;i<=NF;i++){printf "%s\n",$i}}'`
                        do
                                generate_sql "num" $col
                        done
                fi
                # 以上类型合并执行
                exec_sql
        fi
        total_num=`echo $result | awk '{print $1}'`
        echo "total$sp$total_num" | tee $sout_log
        distinct_cnt=0
        if [[ ! -z $distinct_cols ]] && [[  -z $is_max_compare]]
        then
                echo result:$result
                distinct_cnt=`echo $distinct_cols | awk -F , '{for(i=1;i<=NF;i++){printf "%s\n",$i}}' | wc -l`
                distinct_result=`echo $result | awk -v cnt="$distinct_cnt" '{for(i=1;i<=cnt;i++){print $(1+i)}}'`
                distinct_cols_arr=(`echo $distinct_cols | awk -F , '{for(i=1;i<=NF;i++){printf "%s\n",$i}}'`)
                distinct_result_arr=($distinct_result)
                for ((i=0; i<$distinct_cnt; i++))
                do
                        echo distinct@"${distinct_cols_arr[$i]}$sp${distinct_result_arr[$i]}" | tee -a $sout_log
                done
        fi
        if [[ ! -z $num_cols] ] && [[  -z $is_max_compare]]
        then
                num_cnt=`echo $num_cols | awk -F , '{for(i=1;i<=NF;i++){printf "%s\n",$i}}' | wc -l`
                # num_result=`echo $result | awk -v cnt="$num_cnt" -v dcnt="$distinct_cnt" '{tcnt=cnt*4;for(i=1;i<=tcnt;i++){print $(1+dcnt+i)}}'`
                num_result=`echo $result | awk -v cnt="$num_cnt" -v dcnt="$distinct_cnt" '{tcnt=cnt*2;for(i=1;i<=tcnt;i++){print $(1+dcnt+i)}}'`
                num_cols_arr=(`echo $num_cols | awk -F , '{for(i=1;i<=NF;i++){printf "%s\n",$i}}'`)
                num_result_arr=($num_result)
                for ((i=0; i<$num_cnt; i++))
                do
                        # index=`echo "$i*4" | bc`
                        index=`echo "$i*2" | bc`
                        index1=`echo "$index+1" | bc`
                        # index2=`echo "$index+2" | bc`    
                        # index3=`echo "$index+3" | bc`
                        echo number_max@"${num_cols_arr[$i]}$sp${num_result_arr[$index]}" | tee -a $sout_log
                        echo number_min@"${num_cols_arr[$i]}$sp${num_result_arr[$index1]}" | tee -a $sout_log
                        # echo number_avg@"${num_cols_arr[$i]}$sp${num_result_arr[$index2]}" | tee -a $sout_log
                        # echo number_std@"${num_cols_arr[$i]}$sp${num_result_arr[$index3]}" | tee -a $sout_log
                done
        fi
        # 统计枚举分组
        if [ ! -z $enum_cols ]
        then
                for col in `echo $enum_cols | awk -F , '{for(i=1;i<=NF;i++){printf "%s\n",$i}}'`
                do
                        generate_sql "enum" $col
                        # todo:check
                        exec_sql
                        enum=`echo $result | awk '{printf "%s:%s,",$1,$2}' | awk '{print substr($0,0,length($0)-1)}'`
                        echo "enum@$col$sp$enum" | tee -a $sout_log
                done
        fi
        # 统计case when区间结果
        if [[ ! -z $case_sql_file ]] && [[ -f $case_sql_file ]] && [[  -z $is_max_compare]]
        then
                for i in `cat $case_sql_file`
                do
                        generate_sql "case_when" "$i"
                        exec_sql
                done
        fi 
        # 抽样数据md5结果
        if [[ ! -z $sample_num ]] && [[ -n $sample_num ]] && [[  -z $is_max_compare]]
        then
                generate_sql "sample"
                # todo:check
                exec_sql
                md5=`echo $result | md5sum | awk '{print $1}'`
                echo "sample$sp$md5" | tee -a $sout_log
        fi
        upload $sout_log
        end=$(date +%s)  #1599135430
        takes=$(( end - start ))  #开始时间减去结束时间
        take=`echo "scale=2;$takes/60" | bc` #打印出来花费的时间
        curr_time=`date +"%Y-%m-%d %H:%m:%S"`
        echo "$curr_time,$target_table execution take $take mins" | tee -a $exec_log
	if [ $exec_engine == "db2" ]
	then
		rm -rf $db2_tmp
	fi
}

follow_max_compare_name=follow_max_compare_file.config
follow_max_compare_file=/tmp/$follow_max_compare_name

if [ ! -z "$is_follow_max_compare_name" ]
then 
        wait4 $follow_max_compare_name $follow_max_compare_name
        download $follow_max_compare_name  $ollow_max_compare_file
fi

delete /tmp/tabledata_statistic_${bank_name}/$today $ok_file
echo "delete $ok_file"

# 读取配置文件
if [ ! -z $config_file ]
then
        if [ ! -fs $config_file ]
        then
                echo "config_file:$config_file doesn't exists!"
                exit 1
        fi
        # 配置文件结构：new_table#old_table#pk_field#pk_date#out_fields#distinct#number#enum#sample#rate#is_run#add_time

        for conf in `cat $config_file`
        do
                if [ $system_type == "old" ]
                then
                        target_table=`echo $conf | awk -F '#' '{print $2}'`

                else
                        target_table=`echo $conf | awk -F '#' '{print $1}'`
                fi
                pk_field=`echo $conf | awk -F '#' '{print $3}'`
                pk_date=`echo $conf | awk -F '#' '{print $4}'`
                out_fields=`echo $conf | awk -F '#' '{print $5}'`
                distinct_cols=`echo $conf | awk -F '#' '{print $6}'`
                num_cols=`echo $conf | awk -F '#' '{print $7}'`
                enum_cols=`echo $conf | awk -F '#' '{print $8}'`
                sample_num=`echo $conf | awk -F '#' '{print $9}'`
                rate=`echo $conf | awk -F '#' '{print $10}'`
                is_run=`echo $conf | awk -F '#' '{print $11}'`
                add_time_hive=`echo $conf | awk -F '#' '{print $12}'`
                add_time_db2=`echo $conf | awk -F '#' '{print $13}'`
                where_sql=`echo $conf | awk -F '#' '{print $14}'`
                echo "=======where_sql:$where_sql============="
		if [ "$is_run" == "n" ]
		then
			echo "$target_table has been excepted!"
			continue
		fi
                echo target_table:$target_table
                if [ ! -z "$is_follow_max_compare" ]
                then
                        cnt=`cat $follow_max_compare_file | grep $target_table | wc -l`
                        if [ $cnt -gt 0 ]
                        then
                                echo"current table $target_table didn't pass the total count check,don't need to check the columns detail"
                                continue
                        fi
                fi
                statistic
        done
        new_cnt=`ls -al $dir | grep new | wc -l`
        old_cnt=`ls -al $dir | grep old | wc -l`
        echo "new_cnt:$new_cnt,old_cnt:$old_cnt,err_cnt:$err_cnt,see $err_log for execute err tables,see $exec_log for execution time."
        echo "upload ok file"
        echo "done" > $ok_file
        upload $ok_file
else
        statistic
fi

if [ $exec_engine == "db2" ]
then
        db2 connect reset > /dev/null
        echo "reset db2 connect"
fi
if [ ! -z "$is_max_compare" ]
then 
        if [ $exec_engine == "hive" ]
        then
                #-m
                echo "sh $basedir/tabledata_very_report.sh -y $bank_name -f $config_file -m"
                sh $basedir/tabledata_very_report.sh -y $bank_name -f $config_file -m > /tmp/tabledata_verify_$bank_name/sum_$today.out
                #-z
                echo "sh $basedir/tabledata_statistic_report -x hive  -a new -y $bank_name -f $config_file -z"
                sh $basedir/tabledata_statistic_report -x hive  -a new -y $bank_name -f $config_file -z >>/tmp/tabledata_verify_$bank_name/sum_$today.out
                # -z
                sh $basedir/tabledata_very_report.sh  -y $bank_name -f $config_file  -z >>  /tmp/tabledata_verify_$bank_name/sum_$today.out
        else
                #-z
                echo "sh $basedir/tabledata_statistic_report -x db2  -a old -y $bank_name -f $config_file -z "
                sh $basedir/tabledata_statistic_report -x db2  -a old -y $bank_name -f $config_file -z 
                if [ $exec_engine == "hive" ]
                then
                        echo "sh $basedir/tabledata_very_report.sh -y $bank_name -f $config_file"
                        if [ ! -z $config_file ]
                        then 
                        sh $basedir/tabledata_very_report.sh -y $bank_name -f $config_file
                        fi
                fi
        fi