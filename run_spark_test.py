import run_spark


def test_create_cluster():
    run_spark.create_cluster(aws_region='us-west-2',
                             master_type='m1.large',
                             slave_type='m1.xlarge',
                             num_hosts=2,
                             name='foo',
                             ec2_key_name='us-west-2-yodas-com',
                             ec2_subnet_id='subnet-de620da9',
                             s3_work_bucket='yodas-li-analysis')
