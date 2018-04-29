package cloud.localstack;

import cloud.localstack.lambda.DDBEventParser;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.Record;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Simple implementation of a Java Lambda function executor.
 *
 * @author Waldemar Hummer
 */
@SuppressWarnings("restriction")
public class LambdaExecutor {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.err.println("Usage: java " + LambdaExecutor.class.getSimpleName() +
					" <lambdaClass> <recordsFilePath>");
			System.exit(1);
		}

		String fileContent = readFile(args[1]);
		ObjectMapper reader = new ObjectMapper();
		Map<String,Object> map = reader.readerFor(Map.class).readValue(fileContent);

		List<Map<String,Object>> records = (List<Map<String, Object>>) get(map, "Records");
		Object inputObject = map;

		Object handler = getHandler(args[0]);
		if (records == null) {
			Optional<Object> deserialisedInput = getInputObject(reader, fileContent, handler);
			if (deserialisedInput.isPresent()) {
				inputObject = deserialisedInput.get();
			}
		} else {
			if (records.stream().anyMatch(record -> record.containsKey("Kinesis"))) {
				KinesisEvent kinesisEvent = new KinesisEvent();
				inputObject = kinesisEvent;
				kinesisEvent.setRecords(new LinkedList<>());
				for (Map<String, Object> record : records) {
					KinesisEventRecord r = new KinesisEventRecord();
					kinesisEvent.getRecords().add(r);
					Record kinesisRecord = new Record();
					Map<String, Object> kinesis = (Map<String, Object>) get(record, "Kinesis");
				String dataString = new String(get(kinesis, "Data").toString().getBytes());
				byte[] decodedData = Base64.getDecoder().decode(dataString);
				kinesisRecord.setData(ByteBuffer.wrap(decodedData));
					kinesisRecord.setPartitionKey((String) get(kinesis, "PartitionKey"));
					kinesisRecord.setApproximateArrivalTimestamp(new Date());
					r.setKinesis(kinesisRecord);
				}
			} else if (records.stream().anyMatch(record -> record.containsKey("Sns"))) {
				SNSEvent snsEvent = new SNSEvent();
				inputObject = snsEvent;
				snsEvent.setRecords(new LinkedList<>());
				for (Map<String, Object> record : records) {
					SNSEvent.SNSRecord r = new SNSEvent.SNSRecord();
					snsEvent.getRecords().add(r);
					SNSEvent.SNS snsRecord = new SNSEvent.SNS();
					Map<String, Object> sns = (Map<String, Object>) get(record, "Sns");
					snsRecord.setMessage((String) get(sns, "Message"));
					snsRecord.setMessageAttributes((Map<String, SNSEvent.MessageAttribute>) get(sns, "MessageAttributes"));
					snsRecord.setType("Notification");
					snsRecord.setTimestamp(new DateTime());
					r.setSns(snsRecord);
				}
			} else if (records.stream().filter(record -> record.containsKey("dynamodb")).count() > 0) {

				inputObject = DDBEventParser.parse(records);

			}
			else if (records.stream().anyMatch(record -> record.containsKey("s3"))) {
				List<S3EventNotification.S3EventNotificationRecord> notificationRecords = new LinkedList<>();
				for (Map<String, Object> record : records) {

					String awsRegion = (String) get(record, "awsRegion");
                    String eventName = (String) get(record, "eventName");
                    String eventSource = (String) get(record, "eventSource");
                    String eventTime = (String) get(record, "eventTime");
                    String eventVersion = (String) get(record, "eventVersion");

					Map<String, Object> requestParametersMap = (Map<String, Object>) get(record, "requestParameters");
					String sourceIPAddress = (String) get(requestParametersMap, "sourceIPAddress");
                    S3EventNotification.RequestParametersEntity requestParameters = new S3EventNotification.RequestParametersEntity(sourceIPAddress);

					Map<String, Object> responseElementsMap = (Map<String, Object>) get(record, "responseElements");
					String xAmzId2 = (String) get(responseElementsMap, "xAmzId2");
					String xAmzRequestId = (String) get(responseElementsMap, "xAmzRequestId");
                    S3EventNotification.ResponseElementsEntity responseElements = new S3EventNotification.ResponseElementsEntity(xAmzId2, xAmzRequestId);

					Map<String, Object> s3EntityMap = (Map<String, Object>) get(record, "s3");
					Map<String, Object> bucketMap = (Map<String, Object>) get(s3EntityMap, "bucket"); 
					Map<String, Object> objectMap = (Map<String, Object>) get(s3EntityMap, "object");    
					Map<String, Object> ownerIdentityMap = (Map<String, Object>) get(bucketMap, "ownerIdentity");     

                    String configurationId = (String) get(s3EntityMap, "configurationId");
                    String s3SchemaVersion = (String) get(s3EntityMap, "s3SchemaVersion");

					String ownerIdentityPrincipalId = (String) get(ownerIdentityMap, "principalId");
					S3EventNotification.UserIdentityEntity userIdenityEntity = new S3EventNotification.UserIdentityEntity(ownerIdentityPrincipalId);

					String name = (String) get(bucketMap, "name");
					String arn = (String) get(bucketMap, "arn");
					S3EventNotification.S3BucketEntity s3BucketEntity = new S3EventNotification.S3BucketEntity(name, userIdenityEntity, arn);        

					String key = (String) get(objectMap, "key");
					Long size = Long.valueOf((int) get(objectMap, "size"));
					String eTag = (String) get(objectMap, "eTag");
				    String versionId = (String) get(objectMap, "versionId");
					String sequencer = (String) get(objectMap, "sequencer");
					S3EventNotification.S3ObjectEntity s3ObjectEntity = new S3EventNotification.S3ObjectEntity(key, size, eTag, versionId, sequencer);
					
					S3EventNotification.S3Entity s3Entity = new S3EventNotification.S3Entity(configurationId, s3BucketEntity, s3ObjectEntity, s3SchemaVersion);

					Map<String, Object> userIdentityMap = (Map<String, Object>) get(record, "userIdentity");
					String userIdentityPrincipalId = (String) get(userIdentityMap, "principalId");
                    S3EventNotification.UserIdentityEntity userIdentity = new S3EventNotification.UserIdentityEntity(userIdentityPrincipalId);
					S3EventNotification.S3EventNotificationRecord r = new S3EventNotification.S3EventNotificationRecord(awsRegion, eventName, eventSource, eventTime, eventVersion, requestParameters, responseElements, s3Entity, userIdentity);
					
					notificationRecords.add(r);
				}
		
				S3Event s3Event = new S3Event(notificationRecords);
				inputObject = s3Event;
			}
			//TODO: Support other events (S3, SQS...)
		}

		Context ctx = new LambdaContext();
		if (handler instanceof RequestHandler) {
			Object result = ((RequestHandler<Object, ?>) handler).handleRequest(inputObject, ctx);
			// try turning the output into json
			try {
				result = new ObjectMapper().writeValueAsString(result);
			} catch (JsonProcessingException jsonException) {
				// continue with results as it is
			}
			// The contract with lambci is to print the result to stdout, whereas logs go to stderr
			System.out.println(result);
		} else if (handler instanceof RequestStreamHandler) {
			OutputStream os = new ByteArrayOutputStream();
			((RequestStreamHandler) handler).handleRequest(
				new StringInputStream(fileContent), os, ctx);
			System.out.println(os);
		}
	}

	private static Optional<Object> getInputObject(ObjectMapper mapper, String objectString, Object handler) {
		Optional<Object> inputObject = Optional.empty();
		try {
			Optional<Type> handlerInterface = Arrays.stream(handler.getClass().getGenericInterfaces())
					.filter(genericInterface ->
						((ParameterizedTypeImpl) genericInterface).getRawType().equals(RequestHandler.class))
					.findFirst();
			if (handlerInterface.isPresent()) {
				Class<?> handlerInputType = Class.forName(((ParameterizedTypeImpl) handlerInterface.get())
						.getActualTypeArguments()[0].getTypeName());
				inputObject = Optional.of(mapper.readerFor(handlerInputType).readValue(objectString));
			}
		} catch (Exception genericException) {
			// do nothing
		}
		return inputObject;
	}

	private static Object getHandler(String handlerName) throws NoSuchMethodException, IllegalAccessException,
		InvocationTargetException, InstantiationException, ClassNotFoundException {
		Class<?> clazz = Class.forName(handlerName);
		return clazz.getConstructor().newInstance();
	}

	public static <T> T get(Map<String,T> map, String key) {
		T result = map.get(key);
		if(result != null) {
			return result;
		}
		key = StringUtils.uncapitalize(key);
		result = map.get(key);
		if(result != null) {
			return result;
		}
		return map.get(key.toLowerCase());
	}

	public static String readFile(String file) throws Exception {
		if(!file.startsWith("/")) {
			file = System.getProperty("user.dir") + "/" + file;
		}
		return Files.lines(Paths.get(file), StandardCharsets.UTF_8).collect(Collectors.joining());
	}

}
