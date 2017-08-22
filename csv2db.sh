#!/bin/bash

if [ "$#" -ne 12 ]; then
  echo -e "Err: Illegal number of parameters\n"
  echo "Options
   -s source s3 path to folder with csv files
   -h  db host url
   -P  db port
   -u  db user name
   -p  db pass
   -n  db name

ex: csv2pg.sh -s s3://dm-test-storage/tables -h localhost -P 5432 -u postgres -p proov123 -n proov_db
"
  exit 1
fi

while getopts s:h:P:u:p:n: option
do
# echo option ${option} = $OPTARG
 case "${option}" in
 s) S3URL=${OPTARG};;
 h) HOST=${OPTARG};;
 P) PORT=${OPTARG};;
 u) USER=$OPTARG;;
 p) PASS=$OPTARG;;
 n) DBNAME=$OPTARG;;
 esac
done

CSVPATH=./csv-data
[ -d CSVPATH ] || mkdir -p $CSVPATH

# make sure there is no old csv files
rm -rf $CSVPATH/.csv

# aws-cli shall be configured with the right access and secret keys
# thus (bucket_name+path) will be enough to have precise location
# aws s3 cp s3://dm-test-storage/tables /var/lib/mysql-files/ --recursive --exclude "*" --include "*.csv"
aws s3 cp $S3URL $CSVPATH --recursive --exclude "*" --include "*.csv"
if [ $? -ne 0 ]; then exit $?; fi

# upload to remote SERVER:PORT, using USER, PASS, DBNAME
# table name is implied from .csv file name, first line with col names is ignored
for f in `ls ./csv-data`; do
  echo -e "\nImporting [${f%.*}]..."
  psql -h $HOST -d $DBNAME -U $USER -c "\copy ${f%.*} from '${CSVPATH}/$f' with delimiter as ',' csv header"
  echo -e "==> done"
done
