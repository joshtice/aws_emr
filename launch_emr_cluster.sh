aws emr create-cluster \
  --name udacity-spark-cluster \
  --release-label emr-5.32.0 \
  --applications Name=Spark Name=Zeppelin \
  --ec2-attributes KeyName=<insert key name here> \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --log-uri <insert s3 bucket uri for storing logs> \
  --enable-debugging \
  --use-default-roles

