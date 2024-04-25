import json
import boto3
import random
from boto3.dynamodb.conditions import Key, Attr
import uuid
import csv

dynamodb = boto3.resource('dynamodb')
bank_table = dynamodb.Table('Bank')
cc_bank_table = dynamodb.Table('CCToBankAccountMap')
merchant_table = dynamodb.Table('Merchants')
transaction_table = dynamodb.Table('Transaction')

def lambda_handler(event, context):
    body = json.loads(event['body'])

    if 'merchant_name' in body:
        merchant_name = body['merchant_name']
        if 'merchant_token' in body:
            merchant_token = body['merchant_token']
            if authenticate_merchant(merchant_name, merchant_token) == 'Success':
                response = process_transaction(body)
            else:
                response = 'Error: Merchant not authorized'
        else:
            response = process_transaction(body)
    else:
        response = 'Error: Merchant name not provided'

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

def is_bank_available():
    return random.random() > 0.1

def authenticate_merchant(merchant_name, token):
    try:
        response = merchant_table.get_item(
            Key={
                'Merchant Name': merchant_name,
                'Authentication Token': token
            }
        )
        if 'Item' in response:
            return 'Success'
        else:
            return 'Error: Merchant not authorized'
    except Exception as e:
        print(f'Error: {e}')
        return 'Error: Merchant not authorized'

def process_transaction(transaction_data):
    try:
        if not is_bank_available():
            card_num = transaction_data['cc_num']
            cc_num_last_4 = card_num[-4:]
            transaction_table.put_item(Item={'Merchant Name': transaction_data['merchant_name'], 'Merchant ID': str(uuid.uuid4()),'cc_num': cc_num_last_4, 'Amount': transaction_data['amount'], 'status': False, 'error': 'Bank not available'})
            return 'Error: Bank not available'
        
        bank_name = transaction_data['bank']
        cc_num = transaction_data['cc_num']
        amount = float(transaction_data['amount'])
        transaction_type = transaction_data['card_type']
        
        cc_num_int = int(cc_num)
        result = cc_bank_table.query(KeyConditionExpression=Key('CCNum').eq(cc_num_int))
        
        if not result['Items']:
            return 'Error: credit card not found.'
        
        bankName = result['Items'][0]['BankName']
        if bankName != bank_name:
            return 'Error: bank not found.'
        
        if transaction_type == "Debit":
            return "Error: Debit not valid transaction type"
            
        current_balance_item = result['Items'][0]
        current_balance = float(current_balance_item['CreditUsed'])
        credit_limit = float(current_balance_item['CreditLimit'])
        print(f"Amount: {amount}, Credit Limit: {credit_limit}, Current Balance: {current_balance}")
        if amount > (credit_limit - current_balance):
            print(f"Transaction amount ({amount}) exceeds available credit ({credit_limit - current_balance}).")
            last_4_str = cc_num[-4:]
            last_4 = int(last_4_str)
            status = False
            transaction_table.put_item(Item={'Merchant Name': transaction_data['merchant_name'], 'Merchant ID': str(uuid.uuid4()), 'cc_num': last_4, 'Amount': transaction_data['amount'], 'status': status})
            return 'Declined. Insufficient Funds.'
        else:
            updated_balance = amount + current_balance
            print("Debug: Transaction approved.")
            last_4_str = cc_num[-4:]
            last_4 = int(last_4_str)
            status = True
            cc_bank_table.put_item(Item={'CCNum':cc_num_int, 'BankName': bank_name, 'CreditLimit': int(credit_limit), 'CreditUsed': int(updated_balance)})
            transaction_table.put_item(Item={'Merchant Name': transaction_data['merchant_name'], 'Merchant ID': str(uuid.uuid4()), 'cc_num': last_4, 'Amount': transaction_data['amount'], 'status': status})
            return 'Approved.'
    
    except Exception as e:
        print(f'Error: {e}')
        return 'Error processing transaction.'
        
        


# def lambda_handler(event, context):
#     for record in event['Records']:
#         bucket_name = record['s3']['bucket']['name']
#         file_key = record['s3']['object']['key']
        
#         file_name = file_key.split('/')[-1]
        
#         if file_name == 'BankTable.csv':
#             process_bank_table_csv(bucket_name, file_key)
#         elif file_name == 'BankTable-CCs.csv':
#             process_cc_bank_table_csv(bucket_name, file_key)
#         elif file_name == 'merchant_data.csv':
#             process_merchant_table_csv(bucket_name, file_key)
#         else:
#             print("unknown csv file")

#     return {
#         'statusCode': 200,
#         'body': json.dumps(response)
#     }


def process_bank_table_csv(bucket_name, file_key):
    s3_client = boto3.client('s3')
    csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    
    csv_content = csv_file['Body'].read().decode('utf-8').splitlines()
    csv_reader = csv.DictReader(csv_content)

    for row in csv_reader:
        cc_num = int(row['AccountNum'])
        bank_table.put_item(Item={
            'Bank Name': row['BankName'],
            'CCNum': cc_num,
            'Balance': row['Balance']
        })

def process_cc_bank_table_csv(bucket_name, file_key):
    s3_client = boto3.client('s3')
    csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    
    csv_content = csv_file['Body'].read().decode('utf-8').splitlines()
    csv_reader = csv.DictReader(csv_content)
    

    for row in csv_reader:
        cc_num = int(row['AccountNum'])
        cc_bank_table.put_item(Item={
            'CCNum': cc_num,
            'BankName': row['BankName'],
            'CreditLimit': row['CreditLimit'],
            'CreditUsed': row['CreditUsed']
        })

def process_merchant_table_csv(bucket_name, file_key):
    s3_client = boto3.client('s3')
    csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    
    csv_content = csv_file['Body'].read().decode('utf-8').splitlines()
    csv_reader = csv.DictReader(csv_content)
    
    for row in csv_reader:
        merchant_table.put_item(Item={
            'Merchant Name': row['MerchantName'],
            'Authentication Token': row['Token'],
            'BankName': row['BankName'],
            'AccountNum': row['AccountNum']
        })
