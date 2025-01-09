import operations.EC2Operations;
import operations.S3Operations;
import operations.SQSOperations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class LocalApplication {

    private final S3Operations s3Operations;
    private final SQSOperations sqsOperations;
    private final EC2Operations ec2Operations;

    private final String bucket = "bucket-" + UUID.randomUUID();

    private final String lmQueue = "lm-queue-" + UUID.randomUUID();
    private final String mlQueue = "ml-queue-" + UUID.randomUUID();
    private final String registerQueue = "r-queue-31415926535";

    public LocalApplication() {
        Region region = Region.US_EAST_1;
        this.s3Operations = new S3Operations(region);
        this.sqsOperations = new SQSOperations(region);
        this.ec2Operations = new EC2Operations(region);

        sqsOperations.createQueue(lmQueue);
        sqsOperations.createQueue(mlQueue);
        if (Objects.equals(sqsOperations.getQueueUrl(registerQueue), ""))
            sqsOperations.createQueue(registerQueue);
    }

    public void transferFile(String inputPath) {
        if(!s3Operations.doesBucketExist(bucket)){
            s3Operations.createBucket(bucket);
        }
        s3Operations.uploadFile(bucket, inputPath, new File(inputPath));
        sqsOperations.sendMessage(lmQueue, "s3://" + bucket + "/" + inputPath);
    }

    public void activateManager(int docsPerWorker) {
        s3Operations.uploadFile(bucket, "manager.jar", new File("jars/manager.jar"));
        s3Operations.uploadFile(bucket, "worker.jar", new File("jars/worker.jar"));

        String script = String.format("#!/bin/bash\necho \"Downloading manager.jar\"\ncd /home/ec2-user\naws s3 cp s3://%s/manager.jar .\njava -jar manager.jar %d %s %s %s", bucket, docsPerWorker, lmQueue, mlQueue, bucket);

        String[] managersActive = ec2Operations.getAllInstancesWithTag("type", "manager");
        if (managersActive.length > 0) {
            System.out.println("Manager already active");
            sqsOperations.sendMessage(registerQueue, String.format("manager;%d;%s;%s;%s", docsPerWorker, lmQueue, mlQueue, bucket));
            return;
        }
        ec2Operations.runInstances(script, 1, 1,
                Collections.singletonList(Tag.builder().key("type").value("manager").build()));
    }

    public String getSummaryFile(){
        String summaryFile = null;
        while(true) {
            List<Message> messages = sqsOperations.receiveMessages(mlQueue);
            if (!messages.isEmpty()) {
                System.out.println("Received message: " + messages.get(0).body());
                summaryFile = messages.get(0).body();
                break;
            }
            // wait
            try { Thread.sleep(1000); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }

        return summaryFile;
    }

    public void terminateManager(){
        sqsOperations.sendMessage(lmQueue, "terminate");
    }

    public void localAppCleanup() {
        s3Operations.deleteBucket(bucket);
        sqsOperations.deleteQueue(lmQueue);
        sqsOperations.deleteQueue(mlQueue);
    }

    public void downloadFile(String summaryFile, String outputPath) {
        s3Operations.downloadFile(bucket, summaryFile, new File(outputPath + ".html"));
    }

    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4)
            throw new IllegalArgumentException("Usage: LocalApplication <input-file> <output-file> <n> [terminate]");

        long startTime = System.currentTimeMillis();

        String inputFile = args[0];
        String outputFile = args[1];
        int n = Integer.parseInt(args[2]);
        boolean terminate = args.length == 4 && args[3].equals("terminate");

        LocalApplication la = new LocalApplication();

        la.transferFile(inputFile);

        la.activateManager(n);

        String summaryFile = la.getSummaryFile();
        if (summaryFile != null) {
            System.out.println("Summary file received: " + summaryFile);
            la.downloadFile(summaryFile, outputFile);
        }

        if (terminate) {
            la.terminateManager();

            //wait for terminated conformation
            while (true) {
                Message message = la.sqsOperations.receiveMessage(la.mlQueue);
                if ((message != null && message.body().equals("terminated")) ||
                        la.ec2Operations.getAllInstancesWithTag("type", "manager").length == 0) {
                    System.out.println("Manager terminated");
                    break;
                }

                // wait
                try { Thread.sleep(1000); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }
        }

        la.localAppCleanup();

        long endTime = System.currentTimeMillis();
        System.out.println("Execution time: " + (endTime - startTime) + "ms");
    }
}
