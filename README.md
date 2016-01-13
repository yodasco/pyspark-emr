# pyspark-emr
A toolset to streamline running spark python on EMR

## Running spark on a newly created cluster (production)
This will create a cluster and run your spark code on it, then terminate the cluster

```
python emr_run_spark.py \
  --aws_region=us-west-2 \
  --create_cluster \
  --create_cluster_master_type=m3.xlarge \
  --create_cluster_slave_type=m3.xlarge \
  --create_cluster_num_hosts=1 \
  --create_cluster_ec2_key_name=your-ec2-key-name \
  --create_cluster_ec2_subnet_id=your-subnet \
  --python_path=/path/to/your/python/code \
  --s3_work_bucket=your-s3-bucket \
  --spark_main=your-spark-main-script.py \
  --spark_main_args="arguments to your spark script"
```

## Running spark on existing cluster (cluster reuse - useful for debug)
This will use an existing cluster and run your spark code on it (useful for debug)
```
python emr_run_spark.py \
  --aws_region=us-west-2 \
  --job_flow_id=your-emr-cluster-id \
  --python_path=/path/to/your/python/code \
  --s3_work_bucket=your-s3-bucket \
  --spark_main=your-spark-main-script.py \
  --spark_main_args="arguments to your spark script"
```