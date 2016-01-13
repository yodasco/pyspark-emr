import argparse
import boto3
import subprocess
import getpass
import time

client = boto3.client('emr', region_name='us-west-2')

def add_step_to_job_flow(job_flow_id=None, python_path=None, spark_main=None,
                         spark_main_args=None, s3_source_bucket=None):
  assert(job_flow_id)
  assert(python_path)
  assert(spark_main)
  assert(s3_source_bucket)

  zip_file = 'spark_zip.zip'
  # TODO: Change these subprocess calls to use python native API instead of shell
  subprocess.call('rm /tmp/{}'.format(zip_file), shell=True)
  subprocess.check_call("cd {}; zip -r /tmp/{} . -i '*.py'".format(python_path, zip_file), shell=True)
  sources_rel_path = '{}/{}/{}'.format(getpass.getuser(), job_flow_id, int(time.time()))
  s3sources = 's3://{}/sources/{}'.format(s3_source_bucket, sources_rel_path)
  zip_file_on_s3 = '{}/{}'.format(s3sources, zip_file)
  print 'Storing python sources on {}'.format(s3sources)
  subprocess.check_call('aws s3 cp /tmp/{} {}'.format(zip_file, zip_file_on_s3), shell=True)
  sources_on_host = '/home/hadoop/{}'.format(sources_rel_path)
  zip_file_on_host = '{}/{}'.format(sources_on_host, zip_file)
  spark_main_on_host = '{}/{}'.format(sources_on_host, spark_main)
  spark_main_args = spark_main_args.split() if spark_main_args else ['']
  step_response = client.add_job_flow_steps(
    JobFlowId=job_flow_id,
    Steps=[
      {
        'Name': 'setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
          'Jar': 'command-runner.jar',
          'Args': ['aws', 's3', 'cp', zip_file_on_s3, sources_on_host + '/']
        }
      },
      {
        'Name': 'setup - extract files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
          'Jar': 'command-runner.jar',
          'Args': ['unzip', zip_file_on_host, '-d', sources_on_host]
        }
      },
      {
        'Name': 'run spark '.format(spark_main),
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
          'Jar': 'command-runner.jar',
          'Args': ['spark-submit', '--py-files', zip_file_on_host,
              spark_main_on_host] + spark_main_args
        }
      },
    ]
  )
  print step_response
  step_ids = step_response['StepIds']
  _wait_for_job_flow(job_flow_id, step_ids)


def _wait_for_job_flow(job_flow_id, step_ids=[]):
  while True:
    time.sleep(5)
    cluster = client.describe_cluster(ClusterId=job_flow_id)
    state = cluster['Cluster']['Status']['State']
    p = []
    p.append('Cluster: {}'.format(state))
    all_done = True
    for step_id in step_ids:
      step = client.describe_step(ClusterId=job_flow_id, StepId=step_id)
      step_state = step['Step']['Status']['State']
      step_done = step_state in ['COMPLETED', 'FAILED']
      step_failed = step_state == 'FAILED'
      p.append('{} ({}) - {}'.format(step['Step']['Name'],
                                     step['Step']['Id'],
                                     step_state))
      all_done = all_done and step_done
      if step_failed:
        print '!!! STEP FAILED !!!'
    print '\t'.join(p)
    if all_done:
      print "All done"
      break


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--job_flow_id', help='Job flow ID (EMR cluster) to submit to')
  parser.add_argument('--python_path', help='Path to python files to zip and upload to the server and add to the python path. This should include the python_main file`')
  parser.add_argument('--spark_main', help='Main python file for spark')
  parser.add_argument('--spark_main_args', help='Arguments passed to your spark script')
  parser.add_argument('--s3_source_bucket', help='Name of s3 bucket where sources are uploaded')
  args = parser.parse_args()

  if args.job_flow_id:
    add_step_to_job_flow(job_flow_id=args.job_flow_id,
                         python_path=args.python_path,
                         spark_main=args.spark_main,
                         spark_main_args=args.spark_main_args,
                         s3_source_bucket=args.s3_source_bucket)
  else:
    print "Nothing to do"
    parser.print_help()
