import boto3
client = boto3.client('cloudwatch')

def handler(event, context):
    return client.put_metric_data(
            Namespace = 'Pipes',
            MetricData = [
                {
                    'MetricName': 'NewObjectAdded',
                    'Dimensions': [
                        {
                            'Name': 'S3',
                            'Value': 'successfully'
                            }],
                        'Value': 1, 
                        'Unit': 'None'
                        }
                ]
            )

