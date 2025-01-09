import operations.EC2Operations;
import operations.S3Operations;
import operations.SQSOperations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {

    private final ExecutorService threadPool;

    // 9 machines in parallel is the maximum. 1 manager and 8 workers.
    private static final int MAX_WORKERS = 8;

    private final S3Operations s3Operations;
    private final SQSOperations sqsOperations;
    private final EC2Operations ec2Operations;

    private final List<LocalAppData> localApps;

    private final String mwQueue = "mw-queue-" + UUID.randomUUID();
    private final String wmQueue = "wm-queue-" + UUID.randomUUID();
    private final String registerQueue = "r-queue-31415926535";

    private final List<Integer> workers = new LinkedList<>();

    private final Object lock = new Object();
    private boolean running = true;

    public Manager(int docsPerWorker, String lmQueue, String mlQueue, String bucket) {
        this.threadPool = Executors.newFixedThreadPool(10);

        Region region = Region.US_EAST_1;
        this.s3Operations = new S3Operations(region);
        this.sqsOperations = new SQSOperations(region);
        this.ec2Operations = new EC2Operations(region);

        this.localApps = new LinkedList<>();
        localApps.add(new LocalAppData(docsPerWorker, lmQueue, mlQueue, bucket));

        sqsOperations.createQueue(mwQueue);
        sqsOperations.createQueue(wmQueue);
    }

    public void run() {
        System.out.println("Manager running");
        while (running) {
            registerNewLocalApps();

            for (LocalAppData localApp : localApps) {
                threadPool.submit(() -> processIncomingMessage(localApp));
            }

            try { Thread.sleep(1000); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }

        // Once a terminate message is received, do all remaining jobs
        registerNewLocalApps();
        for (LocalAppData localApp : localApps) {
            threadPool.submit(() -> processIncomingMessage(localApp));
        }

        cleanup();
    }

    public void registerNewLocalApps() {
        sqsOperations.receiveMessages(registerQueue).forEach(this::registerLocalApp);
    }

    public void processIncomingMessage(LocalAppData localApp) {
        Message m = sqsOperations.receiveMessage(localApp.getLmQueue());
        sqsOperations.changeVisibilityTimeout(localApp.getLmQueue(), m.receiptHandle(), 200);
        process(m, localApp);
    }

    public void registerLocalApp(Message message) {
        if (message == null)
            return;

        String body = message.body();
        System.out.println("Registering local app: " + body);

        String[] parts = body.split(";");
        int newDocsPerWorker = Integer.parseInt(parts[1]);
        String newLmQueue = parts[2];
        String newMlQueue = parts[3];
        String newBucket = parts[4];

        LocalAppData localApp = new LocalAppData(newDocsPerWorker, newLmQueue, newMlQueue, newBucket);
        localApps.add(localApp);

        sqsOperations.deleteMessage(registerQueue, message.receiptHandle());
    }

    public void process(Message message, LocalAppData localAppData) {
        if (message == null)
            return;

        if (isTerminateMessage(message)) {
            System.out.println("Received terminate message");
            if (!localApps.remove(localAppData)) {
                System.out.println("Local app not found!");
            }
            sqsOperations.sendMessage(localAppData.getMlQueue(), "terminated");
            running = false;
        } else {
            String body = message.body();
            System.out.println("Processing message: " + body);

            String[] lines = readInputFile(body, localAppData);
            for (String line : lines) {
                sqsOperations.sendMessage(mwQueue, line);
            }

            int workersNeeded = (int) Math.ceil((double) lines.length / localAppData.getDocsPerWorker());
            deployWorkers(workersNeeded, localAppData, message.receiptHandle());
            waitForWorkers(localAppData);

            String summaryFile = writeSummaryFile(localAppData);
            sqsOperations.sendMessage(localAppData.getMlQueue(), summaryFile);
        }

        sqsOperations.deleteMessage(localAppData.getLmQueue(), message.receiptHandle());
    }

    public String[] readInputFile(String messageBody, LocalAppData localApp) {
        String localFilePath = messageBody.split(localApp.getBucket() + "/")[1];
        s3Operations.downloadFile(localApp.getBucket(), localFilePath, new File(localFilePath));
        System.out.println("Downloaded file: " + localFilePath);
        return readFile(localFilePath);
    }

    public void deployWorkers(int workersNeeded, LocalAppData localApp, String receiptHandle) {
        long startTime = System.currentTimeMillis();

        int workersUntilNow = 0;
        while (workersUntilNow < workersNeeded) {

            if (System.currentTimeMillis() - startTime > 10 * 1000) {
                sqsOperations.changeVisibilityTimeout(localApp.getLmQueue(), receiptHandle, 200);
                startTime = System.currentTimeMillis();
            }

            gatherResults(localApp);

            synchronized (lock) {
                if (workers.size() < MAX_WORKERS) {
                    deployWorker(workersUntilNow, localApp.getDocsPerWorker(), localApp.getBucket());
                    workersUntilNow++;
                }
            }
        }
    }

    public void waitForWorkers(LocalAppData localApp) {
        while (!workers.isEmpty()) {
            gatherResults(localApp);
        }
    }

    public String writeSummaryFile(LocalAppData localApp) {
        String summaryFile = "summary-" + UUID.randomUUID() + ".html";
        String resultsFormatted = "<html><body>" + String.join("\n", localApp.getResults()) + "</body></html>";

        // create file locally:
        FileWriter writer;
        try {
            writer = new FileWriter(summaryFile);
            writer.write(resultsFormatted);
            writer.close();
        } catch (IOException e) { e.printStackTrace(); }

        // upload file to s3
        s3Operations.uploadFile(localApp.getBucket(), summaryFile, new File(summaryFile));

        return summaryFile;
    }

    public void deployWorker(int id, int filesCapacity, String bucket){
        workers.add(id);

        String script = String.format("#!/bin/bash\ncd /home/ec2-user\naws s3 cp s3://%s/worker.jar .\njava -jar worker.jar %d %d %s %s %s ", bucket, id, filesCapacity, bucket, mwQueue, wmQueue);

        ec2Operations.runInstances(script, 1, 1,
                Arrays.asList(
                        Tag.builder().key("type").value("worker").build(),
                        Tag.builder().key("worker-id").value(String.valueOf(id) + "-" + bucket).build()
                ));
    }

    public void gatherResults(LocalAppData localApp) {
        System.out.println(workersToString());
        List<Message> messages = sqsOperations.receiveMessages(wmQueue);
        for (Message message : messages) {
            sqsOperations.deleteMessage(wmQueue, message.receiptHandle());
            System.out.println("Received message: " + message.body());

            int id = terminatedMessageID(message);
            if (id != -1) {
                System.out.println("Received terminate message");
                cleanupWorker(id, localApp.getBucket());
                workers.remove((Integer) id);
                continue;
            }

            String row = formatAsHTML(message.body());
            localApp.addResult(row);
        }
    }

    public void cleanupWorker(int id, String bucket) {
        // It will be one instance per worker
        String[] instances = ec2Operations.getAllInstancesWithTag("worker-id", String.valueOf(id) + "-" + bucket);
        for (String instance : instances) {
            ec2Operations.terminateInstance(instance);
        }
    }

    public String workersToString(){
        StringBuilder sb = new StringBuilder();
        for (Integer worker : workers) {
            sb.append(worker).append(";");
        }
        return sb.toString();
    }

    public String formatAsHTML(String line) {
        return "<p>" + line + "</p>";
    }

    public int terminatedMessageID(Message message) {
        if (isTerminateMessage(message))
            return Integer.parseInt(message.body().split("\t")[1]);
        return -1;
    }

    public boolean isTerminateMessage(Message message) {
        return message.body().startsWith("terminate");
    }

    public String[] readFile(String filename) {
        try {
            File file = new File(filename);
            if (!file.exists())
                throw new FileNotFoundException("File not found: " + filename);

            BufferedReader reader = new BufferedReader(new FileReader(file));
            return reader.lines().toArray(String[]::new);
        } catch (Exception e) {
            System.err.println("Error reading file: " + e.getMessage());
            return new String[0];
        }
    }


    public void cleanup() {
        sqsOperations.deleteQueue(mwQueue);
        sqsOperations.deleteQueue(wmQueue);

        threadPool.shutdown();

        String[] managers = ec2Operations.getAllInstancesWithTag("type", "manager");
        for (String manager : managers) {
            ec2Operations.terminateInstance(manager);
        }
    }

    public static void main(String[] args) {
        if (args.length != 4)
            throw new RuntimeException("Usage: Manager <docsPerWorker> <lmQueue> <mlQueue> <bucket>");
        Manager manager = new Manager(Integer.parseInt(args[0]), args[1], args[2], args[3]);
        manager.run();
    }

}
