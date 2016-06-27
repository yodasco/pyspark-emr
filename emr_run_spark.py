import argparse
import boto3
import subprocess
import getpass
import time
import os
import zipfile


def _get_client(aws_region):
    return boto3.client('emr', region_name=aws_region)


def add_step_to_job_flow(job_flow_id=None,
                         python_path=None,
                         spark_main=None,
                         py_files=None,
                         num_of_steps=1,
                         use_mysql=False,
                         spark_main_args=None,
                         s3_work_bucket=None,
                         aws_region=None,
                         send_success_email_to=None):
    assert(job_flow_id)
    assert(aws_region)

    job_flow_name = _create_job_flow_name(spark_main)
    steps = _create_steps(job_flow_name=job_flow_name,
                          python_path=python_path,
                          spark_main=spark_main,
                          py_files=py_files,
                          num_of_steps=num_of_steps,
                          use_mysql=use_mysql,
                          spark_main_args=spark_main_args,
                          s3_work_bucket=s3_work_bucket,
                          send_success_email_to=send_success_email_to)
    client = _get_client(aws_region)
    step_response = client.add_job_flow_steps(
      JobFlowId=job_flow_id,
      Steps=steps
    )
    step_ids = step_response['StepIds']
    print "Created steps: {}".format(step_ids)
    print "job_flow_id: {}".format(job_flow_id)


def _create_job_flow_name(spark_main):
    return '{}.{}.{}'.format(getpass.getuser(),
                             spark_main,
                             time.strftime("%H%M%S", time.gmtime()))


def _ls_recursive(dir, suffix=None):
    files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(dir)) for f in fn]
    if suffix:
        files = filter(lambda f: f.endswith(suffix), files)
    return files


def _create_steps(job_flow_name=None,
                  python_path=None,
                  spark_main=None,
                  py_files=[],
                  num_of_steps=1,
                  spark_main_args=None,
                  s3_work_bucket=None,
                  use_mysql=False,
                  send_success_email_to=None):
    assert(python_path)
    assert(spark_main)
    assert(s3_work_bucket)

    zip_file = 'spark_zip.zip'
    sources_rel_path = job_flow_name
    sources_on_host = '/home/hadoop/{}'.format(sources_rel_path)
    local_zip_file = '/tmp/{}'.format(zip_file)
    python_path_files = _ls_recursive(python_path, '.py')
    with zipfile.ZipFile(local_zip_file, 'w') as myzip:
        for f in python_path_files:
            myzip.write(f)
        if py_files:
          for py_file in py_files:
              if py_file.endswith('.zip'):  # Currently only support sip files
                  with zipfile.ZipFile(py_file, 'r') as openzip:
                      [myzip.writestr(t[0], t[1].read())
                       for t in ((n, openzip.open(n))
                       for n in openzip.namelist())]
    s3sources = 's3://{}/sources/{}'.format(s3_work_bucket, sources_rel_path)
    zip_file_on_s3 = '{}/{}'.format(s3sources, zip_file)
    print 'Storing python sources on {}'.format(s3sources)
    # TODO: Change these subprocess calls to use python native API instead of shell
    subprocess.check_call('aws s3 cp {} {}'.format(local_zip_file, zip_file_on_s3), shell=True)
    zip_file_on_host = '{}/{}'.format(sources_on_host, zip_file)
    spark_main_on_host = '{}/{}'.format(sources_on_host, spark_main)
    # spark_main_args = spark_main_args.split() if spark_main_args else ['']
    packages_to_add = []
    if use_mysql:
        packages_to_add.append('mysql:mysql-connector-java:5.1.39')
    packages = ['--packages'] + packages_to_add if packages_to_add else []

    steps = []
    steps.append({
      'Name': 'setup - copy files',
      'ActionOnFailure': 'CANCEL_AND_WAIT',
      'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['aws', 's3', 'cp', zip_file_on_s3, sources_on_host + '/']
      }
    })
    steps.append({
      'Name': 'setup - extract files',
      'ActionOnFailure': 'CANCEL_AND_WAIT',
      'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['unzip', zip_file_on_host, '-d', sources_on_host]
      }
    })
    for i in range(num_of_steps):
        steps.append({
          'Name': 'run spark {}'.format(spark_main),
          'ActionOnFailure': 'CANCEL_AND_WAIT',
          'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': (['spark-submit'] +
                     packages +
                     ['--py-files', zip_file_on_host, spark_main_on_host] +
                     spark_main_args.format(i).split())
          }
        })

    if send_success_email_to is not None:
        steps.append({
          'Name': 'Send success email to {}'.format(send_success_email_to),
          'ActionOnFailure': 'CONTINUE',
          'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 'ses', 'send-email', '--from', 'ops@yodas.com',
                     '--to', send_success_email_to, '--subject',
                     'EMR COMPLETED SUCCESSFULY', '--text', 'Life is good']
          }
        })

    return steps


def _create_debug_steps(setup_debug):
    if setup_debug:
        return [
          {
            'Name': 'Setup Debugging',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['state-pusher-script']
            }
          },
        ]
    else:
        return []


def create_cluster_and_run_job_flow(create_cluster_master_type=None,
                                    create_cluster_slave_type=None,
                                    create_cluster_num_hosts=1,
                                    create_cluster_ec2_key_name=None,
                                    create_cluster_ec2_subnet_id=None,
                                    create_cluster_setup_debug=None,
                                    create_cluster_keep_alive_when_done=None,
                                    bid_price=None,
                                    python_path=None,
                                    num_of_steps=1,
                                    spark_main=None,
                                    py_files=None,
                                    spark_main_args=None,
                                    s3_work_bucket=None,
                                    use_mysql=False,
                                    aws_region=None,
                                    send_success_email_to=None):
    assert(create_cluster_master_type)
    assert(create_cluster_slave_type)
    assert(aws_region)

    s3_logs_uri = 's3n://{}/logs/{}/'.format(s3_work_bucket, getpass.getuser())
    job_flow_name = _create_job_flow_name(spark_main)
    steps = _create_steps(job_flow_name=job_flow_name,
                          python_path=python_path,
                          spark_main=spark_main,
                          py_files=py_files,
                          num_of_steps=num_of_steps,
                          spark_main_args=spark_main_args,
                          s3_work_bucket=s3_work_bucket,
                          use_mysql=use_mysql,
                          send_success_email_to=send_success_email_to)
    client = _get_client(aws_region)
    debug_steps = _create_debug_steps(create_cluster_setup_debug)
    if bid_price:
        num_cores = create_cluster_num_hosts / 3
        instances = {
                'InstanceGroups': [
                    {
                        'Name': 'EmrMaster',
                        'InstanceRole': 'MASTER',
                        'InstanceType': create_cluster_master_type,
                        'InstanceCount': 1
                        },
                    {
                        'Name': 'EmrCore',
                        'InstanceRole': 'CORE',
                        'InstanceType': create_cluster_slave_type,
                        'InstanceCount': num_cores
                        },
                    {
                        'Name': 'EmrTask',
                        'Market': 'SPOT',
                        'InstanceRole': 'TASK',
                        'BidPrice': bid_price,
                        'InstanceType': create_cluster_slave_type,
                        'InstanceCount': create_cluster_num_hosts - num_cores
                        },
                    ],
                'Ec2KeyName': create_cluster_ec2_key_name,
                'KeepJobFlowAliveWhenNoSteps': create_cluster_keep_alive_when_done,
                'TerminationProtected': False,
                'Ec2SubnetId': create_cluster_ec2_subnet_id
                }
    else:
        instances = {
          'MasterInstanceType': create_cluster_master_type,
          'SlaveInstanceType': create_cluster_slave_type,
          'InstanceCount': create_cluster_num_hosts,
          'Ec2KeyName': create_cluster_ec2_key_name,
          'KeepJobFlowAliveWhenNoSteps': create_cluster_keep_alive_when_done,
          'TerminationProtected': False,
          'Ec2SubnetId': create_cluster_ec2_subnet_id
        }
    response = client.run_job_flow(
        Name=job_flow_name,
        LogUri=s3_logs_uri,
        ReleaseLabel='emr-4.6.0',
        Instances=instances,
        Steps=debug_steps + steps,
        Applications=[{'Name': 'Ganglia'}, {'Name': 'Spark'}],
        Configurations=[
          {
            'Classification': 'spark',
            'Properties': {
               'maximizeResourceAllocation': 'true'
            }
          },
          {
            "Classification": "spark-defaults",
            "Properties": {
               "spark.dynamicAllocation.enabled": "true",
               "spark.executor.instances": "0"
            }
          }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Tags=[{'Key': 'Name', 'Value': spark_main}]
    )
    job_flow_id = response['JobFlowId']
    print 'Created Job Flow: {}'.format(job_flow_id)
    step_ids = _get_step_ids_for_job_flow(job_flow_id, client)
    print 'Created Job steps: {}'.format(step_ids)
    print '''Waiting for steps to finish. Visit on aws portal:
        https://{0}.console.aws.amazon.com/elasticmapreduce/home?region={0}#cluster-details:{1}'''.format(aws_region, job_flow_id)
    print "Find logs here: {0}{1}/".format(s3_logs_uri, job_flow_id)
    return job_flow_id


def _get_step_ids_for_job_flow(job_flow_id, client):
    steps = client.list_steps(ClusterId=job_flow_id)
    step_ids = map(lambda s: s['Id'], steps['Steps'])
    return step_ids


def _wait_for_job_flow(aws_region, job_flow_id, step_ids=[]):
    while True:
        time.sleep(30)
        client = _get_client(aws_region)
        cluster = client.describe_cluster(ClusterId=job_flow_id)
        state = cluster['Cluster']['Status']['State']
        state_failed = state in ['TERMINATED_WITH_ERRORS']
        p = []
        p.append('Cluster: {}'.format(state))
        all_done = True
        for step_id in step_ids:
            step = client.describe_step(ClusterId=job_flow_id, StepId=step_id)
            step_state = step['Step']['Status']['State']
            step_failed = step_state in ['FAILED', 'CANCELLED']
            step_success = step_state in ['COMPLETED']
            step_done = step_success or step_failed
            step_name = step['Step']['Name']
            if not step_success:
                p.append('{} ({}) - {}'.format(step_name, step_id, step_state))
            all_done = all_done and step_done
            if step_failed:
                print '!!! STEP FAILED: {} ({})'.format(step_name, step_id)
        print '\t'.join(p)
        if all_done:
            print "All done"
            break
        if state_failed:
            print ">>>>>>>>>>>>>>>> FAILED <<<<<<<<<<<<<<<<<<"
            print "Error message: {}".format(cluster['Cluster']
                                             ['Status']['Message'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--create_cluster',
                        help='Create a new cluster (and destroy it when it ' +
                             'is done',
                        action='store_true')
    parser.add_argument('--create_cluster_master_type', help='Number of hosts',
                        default='m1.medium')
    parser.add_argument('--create_cluster_slave_type', help='Number of hosts',
                        default='m3.xlarge')
    parser.add_argument('--create_cluster_num_hosts', help='Number of hosts',
                        type=int, default=1)
    parser.add_argument('--create_cluster_ec2_key_name', help='Keyfile when ' +
                        'you want to create a new cluster and connect to it')
    parser.add_argument('--create_cluster_ec2_subnet_id', help='')
    parser.add_argument('--create_cluster_keep_alive_when_done', default=False,
                        action='store_true',
                        help='Terminate the cluster when execution is done')
    parser.add_argument('--create_cluster_setup_debug', default=True,
                        help='Whether to setup the cluster for debugging',
                        action='store_true')

    parser.add_argument('--aws_region', help='AWS region', required=True)

    parser.add_argument('--job_flow_id',
                        help='Job flow ID (EMR cluster) to submit to')
    parser.add_argument('--python_path', required=True,
                        help='Path to python files to zip and upload to the' +
                        ' server and add to the python path. This should ' +
                        'include the python_main file`')
    parser.add_argument('--spark_main', required=True,
                        help='Main python file for spark')
    parser.add_argument('--spark_main_args',
                        help='Arguments passed to your spark script')
    parser.add_argument('--s3_work_bucket', required=True,
                        help='Name of s3 bucket where sources and logs are ' +
                        'uploaded')
    parser.add_argument('--py-files', nargs='*', dest='py_files',
                        help='A list of py or zip or egg files to pass over ' +
                        'to spark-submit')
    parser.add_argument('--use_mysql', default=False,
                        help='Whether to setup mysql dataframes jar',
                        action='store_true')
    parser.add_argument('--send_success_email_to', default=None,
                        help='Email address to send on success')
    parser.add_argument('--num_of_steps', default=1, type=int)
    parser.add_argument('--bid_price', default=None)

    args = parser.parse_args()

    if args.job_flow_id:
        add_step_to_job_flow(job_flow_id=args.job_flow_id,
                             python_path=args.python_path,
                             spark_main=args.spark_main,
                             spark_main_args=args.spark_main_args,
                             num_of_steps=args.num_of_steps,
                             py_files=args.py_files,
                             use_mysql=args.use_mysql,
                             s3_work_bucket=args.s3_work_bucket,
                             aws_region=args.aws_region,
                             send_success_email_to=args.send_success_email_to)
    elif args.create_cluster:
        job_flow_id = create_cluster_and_run_job_flow(
            create_cluster_master_type=args.create_cluster_master_type,
            create_cluster_slave_type=args.create_cluster_slave_type,
            create_cluster_num_hosts=args.create_cluster_num_hosts,
            create_cluster_ec2_key_name=args.create_cluster_ec2_key_name,
            create_cluster_ec2_subnet_id=args.create_cluster_ec2_subnet_id,
            create_cluster_setup_debug=args.create_cluster_setup_debug,
            create_cluster_keep_alive_when_done=args.create_cluster_keep_alive_when_done,
            bid_price=args.bid_price,
            python_path=args.python_path,
            spark_main=args.spark_main,
            py_files=args.py_files,
            use_mysql=args.use_mysql,
            spark_main_args=args.spark_main_args,
            num_of_steps=args.num_of_steps,
            s3_work_bucket=args.s3_work_bucket,
            aws_region=args.aws_region,
            send_success_email_to=args.send_success_email_to)
        with open('.job_flow_id.txt', 'w') as f:
            f.write(job_flow_id)
    else:
        print "Nothing to do"
        parser.print_help()
