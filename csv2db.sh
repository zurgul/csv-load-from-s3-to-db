#!/bin/bash

if [ "$#" -ne 14 ]; then
  echo -e "Err: Illegal number of parameters\n"
  echo "Options
   -t        db type mysql or pg
   -s        source s3 path to folder with csv files
   -h        db host url
   -P        db port
   -u        db user name
   -p        db pass
   -n        db name

ex: csv2db.sh -t pg -s s3://test-storage/tables -h localhost -P 5432 -u postgres -p pass123 -n  my _db
"
  exit 1
fi

while getopts t:s:h:P:u:p:n: option
do
#  echo ${option} ${OPTARG}
 case "${option}" in
 t) DBTYPE=${OPTARG};;
 s) S3URL=${OPTARG};;
 h) HOST=${OPTARG};;
 P) PORT=${OPTARG};;
 u) USER=${OPTARG};;
 p) PASS=${OPTARG};;
 n) DBNAME=${OPTARG};;
 esac
done

DBTYPE_PG='pg'
DBTYPE_MYSQL='mysql'
if [ "$DBTYPE" != "$DBTYPE_PG" ] && [ "$DBTYPE" != "$DBTYPE_MYSQL" ]; then
  echo Err: wrong DBTYPE must be 'pg' or 'mysql'
  exit 1
fi

CSVPATH=./csv-data
[ -d CSVPATH ] || mkdir -p $CSVPATH

# make sure there is no old csv files
rm -rf $CSVPATH/.csv

# aws-cli shall be configured with the right access and secret keys
aws s3 cp $S3URL $CSVPATH --recursive --exclude "*" --include "*.csv"

if [ `ls -1U ./csv-data/*.csv | wc -l` -eq 0 ]; then
    echo Err: CSV files not found at [$S3URL]
    exit 1
fi
if [ $? -ne 0 ]; then exit $?; fi

ERRCODE=0

load2pg() {
  psql -h $HOST -d $DBNAME -U $USER -c "\copy ${1%.*} ($2) from '${CSVPATH}/$1' with delimiter as ',' csv header"
  ERRCODE=$?
}

load2mysql() {
  mysqlimport -h $HOST -P $PORT --fields-terminated-by=, --ignore-lines=1 --columns="$2" --local -u $USER -p$PASS $DBNAME $CSVPATH/$1
  ERRCODE=$?
}

if [ "$DBTYPE" == "$DBTYPE_PG" ]; then
  export PGPASSWORD=$PASS
fi

# table name is implied from .csv file name, first line with col names is ignored
for f in `ls ./csv-data`; do
  echo -e "\nImporting [${f%.*}]..."
  COLUMNS=$(head -n 1 $CSVPATH/$f)

  case "${DBTYPE}" in
    ${DBTYPE_PG})
      load2pg $f $COLUMNS
      ;;
    ${DBTYPE_MYSQL})
      load2mysql $f $COLUMNS
      ;;
  esac

  if [ $ERRCODE -ne 0 ]; then
    echo -e "\nErr: import error"
    exit $ERRCODE
  fi

  echo -e "==> done"
done

echo -e "\nSUCCESS\n"
