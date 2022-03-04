import json
import boto3
import logging
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

TABLE_NAME = 'yelp-restaurants'
SAMPLE_N = '5'
host = 'https://search-diningbot-xxxxxxxxxxxxxxxxxx.us-east-1.es.amazonaws.com/'
queue_url = 'https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxxxxxx/dining-bot-queue'

credentials = boto3.Session().get_credentials()
REGION = 'us-east-1'

awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, 'es',session_token=credentials.token)
sqsclient = boto3.client('sqs')

es = Elasticsearch(
            host,
            http_auth = ('demo', 'XXXXXXXXXXXXX'),
            connection_class = RequestsHttpConnection
            )
      
def sendEmail(email,message):
    # This address must be verified with Amazon SES.
    SENDER = "xxxxxxxxxxxxxxxx@gmail.com"
    RECIPIENT = email
    # The subject line for the email.
    SUBJECT = "Restaurant Recommendations for you!"
    CHARSET = "UTF-8"
    # Create a new SES resource and specify a region.
    client = boto3.client('ses')
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                  
                    'Text': {
                        'Charset': CHARSET,
                        'Data': message,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # If you are not using a configuration set, comment or delete the
            # following line
           
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def search(cuisine):
    data = es.search(index="restaurants", body={"query": {"match": {'categories.title':cuisine}}})
    print(data)
    print("search complete", data['hits']['hits'])
    return data['hits']['hits']

def sendmsg(number,message):
    sns = boto3.client('sns')
    messageSent = sns.publish(
            PhoneNumber= number,
            Message= message,
        )

def get_restaurant_data(ids):
    dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table('yelp-restaurants')
    ans = 'Hi! Here are your suggestions,\n '
    i = 1
    for id in ids:
        if i<6:
            response = table.get_item(
                Key={
                    'id': id
                }
            )
            print(response)
            response_item = response['Item']
            print(response_item)
            restaurant_name = response_item['name']
            restaurant_address = response_item['address']
            # restaurant_city = response_item['city:']
            restaurant_zipcode = response_item['zip_code']
            restaurant_rating = str(response_item['rating'])
            ans += "{}. {}, located at {}\n".format(i, restaurant_name, restaurant_address)
            # return ans
            i += 1
        else:
            break
    print("db pass")
    return ans # string type

def lambda_handler(event=None, context=None):
    messages = sqsclient.receive_message(QueueUrl=queue_url,MaxNumberOfMessages=1, MessageAttributeNames=['All'])
    message = messages['Messages'][0]
    logger.debug(message)
    receipt_handle = message['ReceiptHandle']
    req_attributes = message['MessageAttributes']
    cuisine = req_attributes['Cuisine']['StringValue']
    location = req_attributes['Location']['StringValue']
    dining_date = req_attributes['DiningDate']['StringValue']
    dining_time = req_attributes['DiningTime']['StringValue']
    num_people = req_attributes['PeopleNum']['StringValue']
    email = req_attributes['Email']['StringValue']
    number = req_attributes['PhoneNum']['StringValue']
    num = "+1{}".format(number)

    #print(email,number)
    ids = search(cuisine)
    ids = list(map(lambda x: x['_id'], ids))
    print(ids)
    rest_details = get_restaurant_data(ids)
   
    message = str(rest_details) +"requested for " +str(cuisine) + "for " + str(num_people) + " people, for " + str(dining_date) + " at " + str(dining_time)  +" time in " + str(location)+ "\n\n"+ "Enjoy your meal!"  

    sendEmail(email,message)
    sendmsg(num,message)
    sqsclient.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
            
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda LF2!')
    }
        

lambda_handler()
