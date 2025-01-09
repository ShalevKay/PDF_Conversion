package operations;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.LinkedList;
import java.util.List;

public class SQSOperations {

    private final SqsClient sqsClient;

    public SQSOperations(Region region) {
        this.sqsClient = SqsClient.builder().region(region).build();
    }

    // Queue operations

    public String createQueue(String name) {
        System.out.println("Creating queue " + name);
        return sqsClient.createQueue(
                builder -> builder.queueName(name)
        ).queueUrl();
    }

    public void deleteQueue(String queueName) {
        System.out.println("Deleting queue " + queueName);
        String queueUrl = getQueueUrl(queueName);
        if (queueUrl.isEmpty()) {
            return;
        }

        sqsClient.deleteQueue(
                builder -> builder.queueUrl(queueUrl)
        );
    }

    public String getQueueUrl(String queueName) {
        System.out.println("Getting queue URL for " + queueName);
        try {
            return sqsClient.getQueueUrl(
                    builder -> builder.queueName(queueName)
            ).queueUrl();
        } catch (Exception e) {
            System.out.println("Queue " + queueName + " does not exist");
            return "";
        }
    }

    // Push message

    public void sendMessage(String queueUrl, String message) {
        System.out.println("Sending message '" + message + "' to " + queueUrl);
        sqsClient.sendMessage(
                builder -> builder.queueUrl(queueUrl).messageBody(message)
        );
    }

    // Pop message

    public List<Message> receiveMessages(String queueName) {
        System.out.println("Receiving messages from " + queueName);
        String queueUrl = getQueueUrl(queueName);
        if (queueUrl.isEmpty()) {
            return new LinkedList<>();
        }

        List<Message> response = sqsClient.receiveMessage(
                builder -> builder.queueUrl(queueUrl).maxNumberOfMessages(10)
        ).messages();

        return response;
    }

    public Message receiveMessage(String queueName) {
        System.out.println("Receiving message from " + queueName);
        String queueUrl = getQueueUrl(queueName);
        if (queueUrl.isEmpty()) {
            return null;
        }

        List<Message> response = sqsClient.receiveMessage(
                builder -> builder.queueUrl(queueUrl).maxNumberOfMessages(1)
        ).messages();

        return response.isEmpty() ? null : response.get(0);
    }

    public void deleteMessage(String queueName, String receiptHandle) {
        System.out.println("Deleting message with receipt handle " + receiptHandle + " from " + queueName);
        String queueUrl = getQueueUrl(queueName);
        if (queueUrl.isEmpty()) {
            return;
        }

        sqsClient.deleteMessage(
                builder -> builder.queueUrl(queueUrl).receiptHandle(receiptHandle)
        );
    }

    public void changeVisibilityTimeout(String queueName, String receiptHandle, int timeout) {
        System.out.println("Changing visibility timeout of message with receipt handle " + receiptHandle + " to " + timeout + " seconds");
        String queueUrl = getQueueUrl(queueName);
        if (queueUrl.isEmpty()) {
            return;
        }

        sqsClient.changeMessageVisibility(
                builder -> builder.queueUrl(queueUrl).receiptHandle(receiptHandle).visibilityTimeout(timeout)
        );
    }

}
