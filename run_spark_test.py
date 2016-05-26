import run_spark


def test_create_cluster():
    run_spark.create_cluster(aws_region='us-west-2',
                             master_type='m1.large',
                             slave_type='m1.xlarge',
                             num_hosts=2,
                             bid_price='0.05',
                             name='foo',
                             ec2_key_name='us-west-2-yodas-com',
                             ec2_subnet_id='subnet-de620da9',
                             s3_work_bucket='yodas-li-analysis')


def test_add_steps():
    run_spark.add_step_to_job_flow(job_flow_name=None,
                                   python_path=None,
                                   spark_main=None,
                                   py_files=[],
                                   spark_main_args=None,
                                   s3_work_bucket=None,
                                   use_mysql=False,
                                   send_success_email_to=None)
