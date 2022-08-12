# this script will trigger the python code csv.py

#below is thevariables needed for virtual environment and kerberos authentication
VENV_ZIP_FILE=/users/${USER}/projects/pve/pve27_shared_01.zip
export SPARKSUPPLEMENTFILES="/etc/hive/conf/hive-site.xml,/etc/hbase/conf/hbase-site.xml"
export PYTHONSPARKJOB="csv.py"
export PHOENIX_ZK_URL="hpserver01.onedomain.com,hpserver02.onedomain.com,hpserver03.onedomain.com:2181/hbase:principal=hbase/_HOST@TEST.LOCAL"
export KEYTABNAME="testaccount.service.keytab"
export KEYTABPRINCIPAL="testaccount@onedomain.com"

kinit -kt $KEYTABNAME $KEYTABPRINCIPAL

touch spark_log.txt

#below I am trigerring the script on yarn which will coordinate the tasks inside the cluster
#memory can be increased in case we need to load bigger files
spark-submit --master yarn --driver-memory 5G --num-executors 2 --executor-cores 2 --executor-memory 6G \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/data/pve/pve27_shared_01/bin/python \
--conf spark.yarn.appMasterEnv.PYTHONPATH=/data/pve/pve27_shared_01/bin/python \
--conf spark.pyspark.driver.python=/data/pve/pve27_shared_01/bin/python \
--py-files "$SPARKSUPPLEMENTPYTHONFILES" --conf spark.yarn.maxAppAttempts=1 \
--files "$SPARKSUPPLEMENTFILES"  $PYTHONSPARKJOB \
-zk_url $PHOENIX_ZK_URL | tee spark_log.txt
