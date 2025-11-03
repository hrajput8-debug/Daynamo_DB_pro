import boto3
from botocore.exceptions import ClientError

def getDynamoDBConnection(config=None, endpoint=None, port=None, local=False, use_instance_metadata=False):
    if local:
        return boto3.resource('dynamodb', endpoint_url=f"http://{endpoint}:{port}")
    else:
        return boto3.resource('dynamodb')

def createGamesTable(db):
    try:
        table = db.create_table(
            TableName='Games',
            KeySchema=[
                {'AttributeName': 'GameId', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'GameId', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        table.wait_until_exists()
        print("✅ Games table created.")
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("ℹ️ Games table already exists.")
            return db.Table('Games')
        else:
            raise e
