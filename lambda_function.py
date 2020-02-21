import json
import boto3
import os

def lambda_handler(event, context):
    
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(
        os.environ['device_configuration_table']
    )

    for record in event.get('Records'):
        
        vals = {}
        expr = "set "
        
        # Set 'update'
        update = record.get('body')
    
        # Check if 'update' is a string
        if isinstance(update, str):
            # Change 'data' a dict if a string
            update = json.loads(update)
            
        data = update.get('data')
        topic = update.get('topic')
        
        if topic.split('/')[-1] != data.get('clientId'):
            raise("device_id mismatch")
        
        if data.get('eventType'):
            # Assume device is not connected
            vals[':c'] = False
            expr = expr + "connected = :c"
            
            if data.get('eventType') == 'connected':
                # Update 'connected' if device event is connection event
                vals[':c'] = True
        
        if data.get('timestamp'):
            try:
                # Try to cast the timestamp to an int
                vals[':t'] = int(data.get('timestamp'))
                expr = expr + ", last_update = :t"
            except:
                continue
            
        if data.get('ipAddress'):
            vals[':ip'] = data.get('ipAddress')
            expr = expr + ", ip_address = :ip"
            

        # Update the entries in the DynamoDB table
        response = table.update_item(
            Key={
                'device_id': data.get('clientId')
            },
            UpdateExpression=expr,
            ExpressionAttributeValues=vals,
            ReturnValues="UPDATED_NEW"
        )
        
        # Check the response from Dynamo and raise an error if unsuccessful
        metadata = response.get('ResponseMetadata')
        if metadata.get('HTTPStatusCode') != 200:
            raise("Error")
            
            
        # SQS will not delete the message unless it gets a 200 response, so,
        # only return 200 if the message has been stored
        
    return { 'statusCode': 200 }
