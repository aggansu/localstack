import re
import logging
import json
import uuid
import xmltodict
import collections
import six
from six import iteritems
from six.moves.urllib import parse as urlparse
import botocore.config
from requests.models import Response, Request
from localstack.constants import DEFAULT_REGION
from localstack.config import HOSTNAME, HOSTNAME_EXTERNAL
from localstack.utils import persistence
from localstack.utils.aws import aws_stack
from localstack.utils.common import short_uid, timestamp, TIMESTAMP_FORMAT_MILLIS, to_str, to_bytes, clone, md5
from localstack.utils.analytics import event_publisher
from localstack.services.generic_proxy import ProxyListener
from localstack.services.s3 import multipart_content

# mappings for S3 bucket notifications
S3_NOTIFICATIONS = {}

# mappings for bucket CORS settings
BUCKET_CORS = {}

# mappings for bucket lifecycle settings
BUCKET_LIFECYCLE = {}

# mappings for bucket user defined metadata
BUCKET_USER_DEFINED_METADATA = {}

# set up logger
LOGGER = logging.getLogger(__name__)

# XML namespace constants
XMLNS_S3 = 'http://s3.amazonaws.com/doc/2006-03-01/'

# list of destination types for bucket notifications
NOTIFICATION_DESTINATION_TYPES = ('Queue', 'Topic', 'CloudFunction', 'LambdaFunction')


def event_type_matches(events, action, api_method):
    """ check whether any of the event types in `events` matches the
        given `action` and `api_method`, and return the first match. """
    for event in events:
        regex = event.replace('*', '[^:]*')
        action_string = 's3:%s:%s' % (action, api_method)
        match = re.match(regex, action_string)
        if match:
            return match
    return False


def filter_rules_match(filters, object_path):
    """ check whether the given object path matches all of the given filters """
    filters = filters or {}
    s3_filter = _get_s3_filter(filters)
    for rule in s3_filter.get('FilterRule', []):
        if rule['Name'] == 'prefix':
            if not prefix_with_slash(object_path).startswith(prefix_with_slash(rule['Value'])):
                return False
        elif rule['Name'] == 'suffix':
            if not object_path.endswith(rule['Value']):
                return False
        else:
            LOGGER.warning('Unknown filter name: "%s"' % rule['Name'])
    return True


def _get_s3_filter(filters):
    return filters.get('S3Key', filters.get('Key', {}))


def prefix_with_slash(s):
    return s if s[0] == '/' else '/%s' % s


def get_event_message(event_name, bucket_name, file_name='testfile.txt', file_size=1024):
    LOGGER.debug('get_event_message - event_name: "%s"' % event_name)
    LOGGER.debug('get_event_message - bucket_name: "%s"' % bucket_name)
    LOGGER.debug('get_event_message - file_name: "%s"' % file_name)
    # Based on: http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
    return {
        'Records': [{
            'eventVersion': '2.0',
            'eventSource': 'aws:s3',
            'awsRegion': DEFAULT_REGION,
            'eventTime': timestamp(format=TIMESTAMP_FORMAT_MILLIS),
            'eventName': event_name,
            'userIdentity': {
                'principalId': 'AIDAJDPLRKLG7UEXAMPLE'
            },
            'requestParameters': {
                'sourceIPAddress': '127.0.0.1'  # TODO determine real source IP
            },
            'responseElements': {
                'x-amz-request-id': short_uid(),
                'x-amz-id-2': 'eftixk72aD6Ap51TnqcoF8eFidJG9Z/2'  # Amazon S3 host that processed the request
            },
            's3': {
                's3SchemaVersion': '1.0',
                'configurationId': 'testConfigRule',
                'bucket': {
                    'name': bucket_name,
                    'ownerIdentity': {
                        'principalId': 'A3NL1KOZZKExample'
                    },
                    'arn': 'arn:aws:s3:::%s' % bucket_name
                },
                'object': {
                    'key': file_name,
                    'size': file_size,
                    'eTag': 'd41d8cd98f00b204e9800998ecf8427e',
                    'versionId': '096fKKXTRTtl3on89fVO.nfljtsv6qko',
                    'sequencer': '0055AED6DCD90281E5'
                }
            }
        }]
    }


def queue_url_for_arn(queue_arn):
    sqs_client = aws_stack.connect_to_service('sqs')
    parts = queue_arn.split(':')
    return sqs_client.get_queue_url(QueueName=parts[5],
        QueueOwnerAWSAccountId=parts[4])['QueueUrl']


def send_notifications(method, bucket_name, object_path):
    LOGGER.debug('send_notifications - method: "%s"' % method)
    LOGGER.debug('send_notifications - bucket_name: "%s"' % bucket_name)
    LOGGER.debug('send_notifications - object_path: "%s"' % object_path)
    for bucket, config in iteritems(S3_NOTIFICATIONS):
        LOGGER.debug('send_notifications - iteritems loop - bucket: "%s"' % bucket)
        if bucket == bucket_name:
            LOGGER.debug('send_notifications - iteritems loop - bucket match: "%s"' % bucket_name)
            action = {'PUT': 'ObjectCreated', 'POST': 'ObjectCreated', 'DELETE': 'ObjectRemoved'}[method]
            # TODO: support more detailed methods, e.g., DeleteMarkerCreated
            # http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
            api_method = {'PUT': 'Put', 'POST': 'Post', 'DELETE': 'Delete'}[method]
            event_name = '%s:%s' % (action, api_method)
            LOGGER.debug('send_notifications - iteritems loop - event_name: "%s"' % event_name)
            if (event_type_matches(config['Event'], action, api_method) and
                    filter_rules_match(config.get('Filter'), object_path)):
                LOGGER.debug('send_notifications - construct message - event_name: "%s"' % event_name)
                # send notification
                message = get_event_message(
                    event_name=event_name, bucket_name=bucket_name,
                    file_name=urlparse.urlparse(object_path[1:]).path
                )
                message = json.dumps(message)
                if config.get('Queue'):
                    sqs_client = aws_stack.connect_to_service('sqs')
                    try:
                        queue_url = queue_url_for_arn(config['Queue'])
                        sqs_client.send_message(QueueUrl=queue_url, MessageBody=message)
                    except Exception as e:
                        LOGGER.warning('Unable to send notification for S3 bucket "%s" to SQS queue "%s": %s' %
                            (bucket_name, config['Queue'], e))
                if config.get('Topic'):
                    sns_client = aws_stack.connect_to_service('sns')
                    try:
                        sns_client.publish(TopicArn=config['Topic'], Message=message, Subject='Amazon S3 Notification')
                    except Exception as e:
                        LOGGER.warning('Unable to send notification for S3 bucket "%s" to SNS topic "%s".' %
                            (bucket_name, config['Topic']))
                # CloudFunction and LambdaFunction are semantically identical
                lambda_function_config = config.get('CloudFunction') or config.get('LambdaFunction')
                LOGGER.debug('send_notifications - invoke? - lambda_function_config: "%s"' % lambda_function_config)
                if lambda_function_config:
                    # make sure we don't run into a socket timeout
                    connection_config = botocore.config.Config(read_timeout=300)
                    lambda_client = aws_stack.connect_to_service('lambda', config=connection_config)
                    try:
                        lambda_client.invoke(FunctionName=lambda_function_config,
                                             InvocationType='Event', Payload=message)
                    except Exception as e:
                        LOGGER.warning('Unable to send notification for S3 bucket "%s" to Lambda function "%s".' %
                            (bucket_name, lambda_function_config))
                if not filter(lambda x: config.get(x), NOTIFICATION_DESTINATION_TYPES):
                    LOGGER.warning('Neither of %s defined for S3 notification.' %
                        '/'.join(NOTIFICATION_DESTINATION_TYPES))


def get_cors(bucket_name):
    response = Response()
    cors = BUCKET_CORS.get(bucket_name)
    if not cors:
        # TODO: check if bucket exists, otherwise return 404-like error
        cors = {
            'CORSConfiguration': []
        }
    body = xmltodict.unparse(cors)
    response._content = body
    response.status_code = 200
    return response


def set_cors(bucket_name, cors):
    # TODO: check if bucket exists, otherwise return 404-like error
    if not isinstance(cors, dict):
        cors = xmltodict.parse(cors)
    BUCKET_CORS[bucket_name] = cors
    response = Response()
    response.status_code = 200
    return response


def delete_cors(bucket_name):
    # TODO: check if bucket exists, otherwise return 404-like error
    BUCKET_CORS.pop(bucket_name, {})
    response = Response()
    response.status_code = 200
    return response


def append_cors_headers(bucket_name, request_method, request_headers, response):
    cors = BUCKET_CORS.get(bucket_name)
    if not cors:
        return
    origin = request_headers.get('Origin', '')
    rules = cors['CORSConfiguration']['CORSRule']
    if not isinstance(rules, list):
        rules = [rules]
    for rule in rules:
        # add allow-origin header
        allowed_methods = rule.get('AllowedMethod', [])
        if request_method in allowed_methods:
            allowed_origins = rule.get('AllowedOrigin', [])
            for allowed in allowed_origins:
                if origin in allowed or re.match(allowed.replace('*', '.*'), origin):
                    response.headers['Access-Control-Allow-Origin'] = origin
                    break
        # add additional headers
        exposed_headers = rule.get('ExposeHeader', [])
        for header in exposed_headers:
            if header.lower() == 'date':
                response.headers[header] = timestamp(format='%a, %d %b %Y %H:%M:%S +0000')
            elif header.lower() == 'etag':
                response.headers[header] = md5(response._content)
            elif header.lower() in ('server', 'x-amz-id-2', 'x-amz-request-id'):
                response.headers[header] = short_uid()
            elif header.lower() == 'x-amz-delete-marker':
                response.headers[header] = 'false'
            elif header.lower() == 'x-amz-version-id':
                # TODO: check whether bucket versioning is enabled and return proper version id
                response.headers[header] = 'null'

def get_unique_id(bucket_name, object_path):
    LOGGER.debug('get_unique_id - bucket_name: "%s"' % bucket_name)
    LOGGER.debug('get_unique_id - object_path: "%s"' % object_path)
    # try pick the bucket_name from the path
    object_path = object_path.split('?')[0]
    LOGGER.debug('get_unique_id - bucket_name: "%s"' % bucket_name)
    LOGGER.debug('get_unique_id - object_path: "%s"' % object_path)
    unique_id = bucket_name + object_path
    LOGGER.debug('get_unique_id - unique_id: "%s"' % unique_id)
    return unique_id

def set_user_defined_metadata(unique_id, query_map):
    LOGGER.debug('set_user_defined_metadata - unique_id: "%s"' % unique_id)
    if not query_map:
        return
    BUCKET_USER_DEFINED_METADATA[unique_id] = {}
    for key in query_map:
        LOGGER.debug('set_user_defined_metadata - key: "%s"' % key)
        LOGGER.debug('set_user_defined_metadata - value: "%s"' % query_map[key])
        if key.startswith('x-amz-meta') and query_map[key] is not None:
            LOGGER.debug('set_user_defined_metadata - add - value: "%s"' % query_map[key][0])
            BUCKET_USER_DEFINED_METADATA[unique_id][key] = query_map[key][0]

def append_user_defined_metadata_headers(unique_id, request_method, request_headers, response):
    LOGGER.debug('append_user_defined_metadata_headers - unique_id: "%s"' % unique_id)
    metadata = BUCKET_USER_DEFINED_METADATA.get(unique_id)
    if not metadata:
        return
    for key in metadata:
        LOGGER.debug('append_user_defined_metadata_headers - key: "%s"' % key)
        LOGGER.debug('append_user_defined_metadata_headers - value: "%s"' % metadata[key])
        response.headers[key] = metadata[key]

def get_lifecycle(bucket_name):
    response = Response()
    lifecycle = BUCKET_LIFECYCLE.get(bucket_name)
    if not lifecycle:
        # TODO: check if bucket exists, otherwise return 404-like error
        lifecycle = {
            'LifecycleConfiguration': []
        }
    body = xmltodict.unparse(lifecycle)
    response._content = body
    response.status_code = 200
    return response


def set_lifecycle(bucket_name, lifecycle):
    # TODO: check if bucket exists, otherwise return 404-like error
    if isinstance(to_str(lifecycle), six.string_types):
        lifecycle = xmltodict.parse(lifecycle)
    BUCKET_LIFECYCLE[bucket_name] = lifecycle
    response = Response()
    response.status_code = 200
    return response


def strip_chunk_signatures(data):
    # For clients that use streaming v4 authentication, the request contains chunk signatures
    # in the HTTP body (see example below) which we need to strip as moto cannot handle them
    #
    # 17;chunk-signature=6e162122ec4962bea0b18bc624025e6ae4e9322bdc632762d909e87793ac5921
    # <payload data ...>
    # 0;chunk-signature=927ab45acd82fc90a3c210ca7314d59fedc77ce0c914d79095f8cc9563cf2c70

    data_new = re.sub(b'(\r\n)?[0-9a-fA-F]+;chunk-signature=[0-9a-f]{64}(\r\n){,2}', b'',
        data, flags=re.MULTILINE | re.DOTALL)
    if data_new != data:
        # trim \r (13) or \n (10)
        for i in range(0, 2):
            if len(data_new) and data_new[0] in (10, 13):
                data_new = data_new[1:]
        for i in range(0, 6):
            if len(data_new) and data_new[-1] in (10, 13):
                data_new = data_new[:-1]
    return data_new


def expand_redirect_url(starting_url, key, bucket):
    """ Add key and bucket parameters to starting URL query string. """
    parsed = urlparse.urlparse(starting_url)
    query = collections.OrderedDict(urlparse.parse_qsl(parsed.query))
    query.update([('key', key), ('bucket', bucket)])

    redirect_url = urlparse.urlunparse((
        parsed.scheme, parsed.netloc, parsed.path,
        parsed.params, urlparse.urlencode(query), None))

    return redirect_url


def get_bucket_name(path, headers):
    parsed = urlparse.urlparse(path)
    LOGGER.debug('get_bucket_name - path: "%s"' % path)
    # try pick the bucket_name from the path
    bucket_name = parsed.path.split('/')[1]
    LOGGER.debug('get_bucket_name - bucket_name: "%s"' % bucket_name)
    host = headers['host']
    LOGGER.debug('get_bucket_name - host: "%s"' % host)
    LOGGER.debug('get_bucket_name - HOSTNAME: "%s"' % HOSTNAME)
    LOGGER.debug('get_bucket_name - HOSTNAME_EXTERNAL: "%s"' % HOSTNAME_EXTERNAL)
    # is the hostname not starting a bucket name?
    if host.startswith(HOSTNAME) or host.startswith(HOSTNAME_EXTERNAL):
        return bucket_name

    # matches the common endpoints like
    #     - '<bucket_name>.s3.<region>.amazonaws.com'
    #     - '<bucket_name>.s3-<region>.amazonaws.com.cn'
    common_pattern = re.compile(r'^(.+)\.s3[.\-][a-z]{2}-[a-z]+-[0-9]{1,}'
                                r'\.amazonaws\.com(\.[a-z]+)?$')
    # matches dualstack endpoints like
    #     - <bucket_name>.s3.dualstack.<region>.amazonaws.com'
    #     - <bucket_name>.s3.dualstack.<region>.amazonaws.com.cn'
    dualstack_pattern = re.compile(r'^(.+)\.s3\.dualstack\.[a-z]{2}-[a-z]+-[0-9]{1,}'
                                   r'\.amazonaws\.com(\.[a-z]+)?$')
    # matches legacy endpoints like
    #     - '<bucket_name>.s3.amazonaws.com'
    #     - '<bucket_name>.s3-external-1.amazonaws.com.cn'
    legacy_patterns = re.compile(r'^(.+)\.s3\.?(-external-1)?\.amazonaws\.com(\.[a-z]+)?$')

    # if any of the above patterns match, the first captured group
    # will be returned as the bucket name
    for pattern in [common_pattern, dualstack_pattern, legacy_patterns]:
        match = pattern.match(host)
        if match:
            bucket_name = match.groups()[0]
            LOGGER.debug('get_bucket_name - match: "%s"' % bucket_name)
            break

    # we're either returning the original bucket_name,
    # or a pattern matched the host and we're returning that name instead
    LOGGER.debug('get_bucket_name - bucket name: "%s"' % bucket_name)
    return bucket_name


class ProxyListenerS3(ProxyListener):

    def forward_request(self, method, path, data, headers):
        LOGGER.debug('forward_request - method: "%s"' % method)
        modified_data = None

        # If this request contains streaming v4 authentication signatures, strip them from the message
        # Related isse: https://github.com/localstack/localstack/issues/98
        # TODO we should evaluate whether to replace moto s3 with scality/S3:
        # https://github.com/scality/S3/issues/237
        if headers.get('x-amz-content-sha256') == 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD':
            modified_data = strip_chunk_signatures(data)

        # POST requests to S3 may include a "${filename}" placeholder in the
        # key, which should be replaced with an actual file name before storing.
        if method == 'POST':
            original_data = modified_data or data
            expanded_data = multipart_content.expand_multipart_filename(original_data, headers)
            if expanded_data is not original_data:
                modified_data = expanded_data

        # If no content-type is provided, 'binary/octet-stream' should be used
        # src: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
        if method == 'PUT' and not headers.get('content-type'):
            headers['content-type'] = 'binary/octet-stream'

        # persist this API call to disk
        persistence.record('s3', method, path, data, headers)

        parsed = urlparse.urlparse(path)
        query = parsed.query
        path = parsed.path
        object_path = parsed.path
        LOGGER.debug('forward_request - query: "%s"' % query)
        LOGGER.debug('forward_request - path: "%s"' % path)
        LOGGER.debug('forward_request - host: "%s"' % headers['host'])
        LOGGER.debug('forward_request - object_path: "%s"' % object_path)

        # Check bucket name in host
        bucket = None
        hostname_parts = headers['host'].split('.')
        if len(hostname_parts) > 1:
            bucket = hostname_parts[0]
        
        # No bucket name in host, check in path.
        if (not bucket or len(bucket) == 0):
            bucket = get_bucket_name(path, headers)
        
        LOGGER.debug('forward_request - Bucket name: "%s"' % bucket)

        query_map = urlparse.parse_qs(query)
        if query == 'notification' or 'notification' in query_map:
            LOGGER.debug('forward_request - query: "%s"' % query)
            response = Response()
            response.status_code = 200
            if method == 'GET':
                # TODO check if bucket exists
                result = '<NotificationConfiguration xmlns="%s">' % XMLNS_S3
                if bucket in S3_NOTIFICATIONS:
                    notif = S3_NOTIFICATIONS[bucket]
                    for dest in NOTIFICATION_DESTINATION_TYPES:
                        if dest in notif:
                            dest_dict = {
                                '%sConfiguration' % dest: {
                                    'Id': uuid.uuid4(),
                                    dest: notif[dest],
                                    'Event': notif['Event'],
                                    'Filter': notif['Filter']
                                }
                            }
                            result += xmltodict.unparse(dest_dict, full_document=False)
                result += '</NotificationConfiguration>'
                response._content = result

            if method == 'PUT':
                LOGGER.debug('forward_request - method: "%s"' % method)
                parsed = xmltodict.parse(data)
                notif_config = parsed.get('NotificationConfiguration')
                S3_NOTIFICATIONS.pop(bucket, None)
                for dest in NOTIFICATION_DESTINATION_TYPES:
                    LOGGER.debug('forward_request - NOTIFICATION_DESTINATION_TYPES - dest: "%s"' % dest)
                    config = notif_config.get('%sConfiguration' % (dest))
                    if config:
                        events = config.get('Event')
                        if isinstance(events, six.string_types):
                            events = [events]
                        event_filter = config.get('Filter', {})
                        # make sure FilterRule is an array
                        s3_filter = _get_s3_filter(event_filter)
                        if s3_filter and not isinstance(s3_filter.get('FilterRule', []), list):
                            s3_filter['FilterRule'] = [s3_filter['FilterRule']]
                        # create final details dict
                        notification_details = {
                            'Id': config.get('Id'),
                            'Event': events,
                            dest: config.get(dest),
                            'Filter': event_filter
                        }
                        # TODO: what if we have multiple destinations - would we overwrite the config?
                        LOGGER.debug('forward_request - S3_NOTIFICATIONS - bucket: "%s"' % bucket)
                        S3_NOTIFICATIONS[bucket] = clone(notification_details)

            # return response for ?notification request
            return response

        if query == 'cors' or 'cors' in query_map:
            if method == 'GET':
                return get_cors(bucket)
            if method == 'PUT':
                return set_cors(bucket, data)
            if method == 'DELETE':
                return delete_cors(bucket)

        if query == 'lifecycle' or 'lifecycle' in query_map:
            if method == 'GET':
                return get_lifecycle(bucket)
            if method == 'PUT':
                return set_lifecycle(bucket, data)

        LOGGER.debug('forward_request - query_map: "%s"' % query_map)
        if method == 'PUT' and 'x-amz-meta-filename' in query_map:
            unique_id = get_unique_id(bucket, object_path)
            set_user_defined_metadata(unique_id, query_map)

        if modified_data:
            return Request(data=modified_data, headers=headers, method=method)
        return True

    def return_response(self, method, path, data, headers, response):

        LOGGER.debug('return_response - host: "%s"' % headers['host'])

        # Check bucket name in host
        bucket_name = None
        hostname_parts = headers['host'].split('.')
        if len(hostname_parts) > 1:
            bucket_name = hostname_parts[0]
        
        # No bucket name in host, check in path.
        if (not bucket_name or len(bucket_name) == 0):
            bucket_name = get_bucket_name(path, headers)
        
        LOGGER.debug('return_response - Bucket name: "%s"' % bucket_name)

        # POST requests to S3 may include a success_action_redirect field,
        # which should be used to redirect a client to a new location.
        key = None
        if method == 'POST':
            key, redirect_url = multipart_content.find_multipart_redirect_url(data, headers)

            if key and redirect_url:
                response.status_code = 303
                response.headers['Location'] = expand_redirect_url(redirect_url, key, bucket_name)
                LOGGER.debug('S3 POST {} to {}'.format(response.status_code, response.headers['Location']))

        LOGGER.debug('return_response - path: "%s"' % path)
        parsed = urlparse.urlparse(path)

        bucket_name_in_host = headers['host'].startswith(bucket_name)
        LOGGER.debug('return_response - bucket_name_in_host: "%s"' % bucket_name_in_host)
        LOGGER.debug('return_response - method: "%s"' % method)
        LOGGER.debug('return_response - parsed.query: "%s"' % parsed.query)
        should_send_notifications = all([
            method in ('PUT', 'POST', 'DELETE'),
            '/' in path[1:] or bucket_name_in_host,
            # check if this is an actual put object request, because it could also be
            # a put bucket request with a path like this: /bucket_name/
            bucket_name_in_host or (len(path[1:].split('/')) > 1 and len(path[1:].split('/')[1]) > 0),
            # don't send notification if url has a query part (some/path/with?query)
            # (query can be one of 'notification', 'lifecycle', 'tagging', etc)
            # Send notification if parsed.query startswith x-amz-meta-filename
            not (parsed.query is None) and 'x-amz-meta-filename' in parsed.query
        ])

        LOGGER.debug('return_response - should_send_notifications: "%s"' % should_send_notifications)
        # get subscribers and send bucket notifications
        if should_send_notifications:
            # if we already have a good key, use it, otherwise examine the path
            if key:
                object_path = '/' + key
            elif bucket_name_in_host:
                object_path = parsed.path
            else:
                parts = parsed.path[1:].split('/', 1)
                object_path = parts[1] if parts[1][0] == '/' else '/%s' % parts[1]
            LOGGER.debug('return_response - send_notifications: method "%s"' % method)
            LOGGER.debug('return_response - send_notifications: bucket_name "%s"' % bucket_name)
            LOGGER.debug('return_response - send_notifications: object_path "%s"' % object_path)
            send_notifications(method, bucket_name, object_path)

        # publish event for creation/deletion of buckets:
        # if method in ('PUT', 'DELETE') and ('/' not in path[1:] or len(path[1:].split('/')[1]) <= 0):
        #    event_type = (event_publisher.EVENT_S3_CREATE_BUCKET if method == 'PUT'
        #        else event_publisher.EVENT_S3_DELETE_BUCKET)
        #    event_publisher.fire_event(event_type, payload={'n': event_publisher.get_hash(bucket_name)})

        LOGGER.debug('return_response - After publish event for creation/deletion of buckets: method "%s"' % method)
        # fix an upstream issue in moto S3 (see https://github.com/localstack/localstack/issues/382)
        if method == 'PUT' and parsed.query == 'policy':
            response._content = ''
            response.status_code = 204
            return response

        LOGGER.debug('return_response - After method == PUT and parsed.query == policy: method "%s"' % method)
        LOGGER.debug('return_response - After method == PUT: response "%s"' % response)
        if response:
            # append CORS headers to response
            unique_id = get_unique_id(bucket_name, object_path)
            append_cors_headers(unique_id, request_method=method, request_headers=headers, response=response)
            LOGGER.debug('return_response - After append_cors_headers: response "%s"' % response)

            # append user defined metadata headers to response
            append_user_defined_metadata_headers(bucket_name, request_method=method, request_headers=headers, response=response)

            LOGGER.debug('return_response - After append_user_defined_metadata_headers: response "%s"' % response)
            response_content_str = None
            try:
                response_content_str = to_str(response._content)
            except Exception:
                pass

            # we need to un-pretty-print the XML, otherwise we run into this issue with Spark:
            # https://github.com/jserver/mock-s3/pull/9/files
            # https://github.com/localstack/localstack/issues/183
            # Note: yet, we need to make sure we have a newline after the first line: <?xml ...>\n
            if response_content_str and response_content_str.startswith('<'):
                is_bytes = isinstance(response._content, six.binary_type)
                response._content = re.sub(r'([^\?])>\n\s*<', r'\1><', response_content_str, flags=re.MULTILINE)
                if is_bytes:
                    response._content = to_bytes(response._content)
                # fix content-type: https://github.com/localstack/localstack/issues/618
                #                   https://github.com/localstack/localstack/issues/549
                if 'text/html' in response.headers.get('Content-Type', ''):
                    response.headers['Content-Type'] = 'application/xml; charset=utf-8'

                response.headers['content-length'] = len(response._content)

            # update content-length headers (fix https://github.com/localstack/localstack/issues/541)
            if method == 'DELETE':
                response.headers['content-length'] = len(response._content)

            LOGGER.debug('return_response - before return: response "%s"' % response)
            return response

# instantiate listener
UPDATE_S3 = ProxyListenerS3()
