import json
import base64
import boto3
import pytz
from datetime import datetime
from botocore.exceptions import ClientError

# AWS Clients
dynamodb = boto3.client('dynamodb', region_name='us-east-1')

DYNAMO_TABLE = 'ProcessedCTR'
EASTERN_TZ = pytz.timezone("America/New_York")


def is_duplicate(contact_id: str) -> bool:
    """Check if ContactId was already processed using DynamoDB conditional write."""
    processed_at = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(EASTERN_TZ).isoformat()

    try:
        dynamodb.put_item(
            TableName=DYNAMO_TABLE,
            Item={
                'ContactId': {'S': contact_id},
                'ProcessedAt': {'S': processed_at}
            },
            ConditionExpression='attribute_not_exists(ContactId)'
        )
        return False  # Not a duplicate
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return True  # Duplicate
        raise  # Re-raise other errors


def parse_datetime(timestamp: str | None) -> str | None:
    """Convert UTC ISO timestamp to America/New_York format."""
    if not timestamp:
        return None
    try:
        dt_utc = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        dt_utc = dt_utc.replace(tzinfo=pytz.utc)
        return dt_utc.astimezone(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def lambda_handler(event: dict, context: dict) -> dict:
    output = []

    for record in event.get('records', []):
        record_id = record['recordId']
        b64_data = record.get('data', '')

        if not b64_data.strip():
            output.append({'recordId': record_id, 'result': 'Dropped', 'data': record['data']})
            continue

        try:
            # Decode and parse CTR
            data_str = base64.b64decode(b64_data).decode('utf-8')
            payload = json.loads(data_str)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"JSON decode error for record {record_id}: {e}")
            output.append({'recordId': record_id, 'result': 'Dropped', 'data': record['data']})
            continue

        contact_id = payload.get('ContactId', '')
        if not contact_id:
            output.append({'recordId': record_id, 'result': 'Dropped', 'data': record['data']})
            continue

        # Deduplication
        if is_duplicate(contact_id):
            print(f"Duplicate ContactId detected: {contact_id}")
            output.append({'recordId': record_id, 'result': 'Dropped', 'data': record['data']})
            continue

        # Extract nested data
        agent = payload.get('Agent', {}) or {}
        queue = payload.get('Queue', {}) or {}
        customer_endpoint = payload.get('CustomerEndpoint', {}) or {}
        system_endpoint = payload.get('SystemEndpoint', {}) or {}

        # Flatten the record
        transformed = {
            'init_contact_id': payload.get('InitialContactId', ''),
            'prev_contact_id': payload.get('PreviousContactId', ''),
            'contact_id': contact_id,
            'next_contact_id': payload.get('NextContactId', ''),
            'channel': payload.get('Channel', ''),
            'init_method': payload.get('InitiationMethod', ''),
            'init_time': parse_datetime(payload.get('InitiationTimestamp')),
            'disconn_time': parse_datetime(payload.get('DisconnectTimestamp')),
            'disconn_reason': payload.get('DisconnectReason', ''),
            'last_update_time': parse_datetime(payload.get('LastUpdateTimestamp')),
            'agent_conn': parse_datetime(agent.get('ConnectedToAgentTimestamp')),
            'agent_id': agent.get('ARN', '').split('/agent/')[-1] if agent.get('ARN') else None,
            'agent_username': agent.get('Username', ''),
            'agent_conn_att': payload.get('AgentConnectionAttempts', 0),
            'agent_afw_start': parse_datetime(agent.get('AfterContactWorkStartTimestamp')),
            'agent_afw_end': parse_datetime(agent.get('AfterContactWorkEndTimestamp')),
            'agent_afw_duration': agent.get('AfterContactWorkDuration', 0),
            'agent_interact_duration': agent.get('AgentInteractionDuration', 0),
            'agent_holds': agent.get('NumberOfHolds', 0),
            'agent_longest_hold': agent.get('LongestHoldDuration', 0),
            'queue_id': queue.get('ARN', '').split('/queue/')[-1] if queue.get('ARN') else None,
            'queue_name': queue.get('Name', ''),
            'in_queue_time': parse_datetime(queue.get('EnqueueTimestamp')),
            'out_queue_time': parse_datetime(queue.get('DequeueTimestamp')),
            'queue_duration': queue.get('Duration', 0),
            'customer_phone': customer_endpoint.get('Address', ''),
            'customer_voice': customer_endpoint.get('Voice', ''),
            'customer_hold_duration': agent.get('CustomerHoldDuration', 0),
            'sys_phone': system_endpoint.get('Address', ''),
            'conn_to_sys': parse_datetime(payload.get('ConnectedToSystemTimestamp')),
        }

        # Encode back for Firehose
        transformed_json = json.dumps(transformed)
        encoded_data = base64.b64encode(transformed_json.encode('utf-8')).decode('utf-8')

        output.append({
            'recordId': record_id,
            'result': 'Ok',
            'data': encoded_data
        })

    return {'records': output}
