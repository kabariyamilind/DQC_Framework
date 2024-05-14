import boto3
class SendAlert():
    def __init__(self):
        pass

    def sendAlertOnSNS(self,subject,message):
        sns = boto3.client('sns', region_name='ap-south-1')
        topic_arn = 'arn:aws:sns:ap-south-1::topic_name'

        # subject = 'DQC_Failure_Alert for DQC Job ' +
        # # Message to be sent
        # message = 'Hello, this is a test notification from SNS.'

        # Publish the message to the topic
        print(subject)
        response = sns.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject
        )
