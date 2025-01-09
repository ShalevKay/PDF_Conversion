import operations.EC2Operations;
import operations.S3Operations;
import operations.SQSOperations;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;

import org.apache.pdfbox.text.PDFTextStripper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;

public class Worker {

    private final S3Operations s3Operations;
    private final SQSOperations sqsOperations;
    private final EC2Operations ec2Operations;

    private final int id;
    private final int docsCapacity;
    private int docsWorkedOn;
    private final String bucket;
    private final String mwQueue;
    private final String wmQueue;

    public Worker(int id, int docsCapacity, String bucket, String mwQueue, String wmQueue) {
        this.id = id;

        Region region = Region.US_EAST_1;
        this.s3Operations = new S3Operations(region);
        this.sqsOperations = new SQSOperations(region);
        this.ec2Operations = new EC2Operations(region);

        this.docsCapacity = docsCapacity;
        this.docsWorkedOn = 0;

        this.bucket = bucket;
        this.mwQueue = mwQueue;
        this.wmQueue = wmQueue;
    }

    public static void main(String[] args) {
        if (args.length != 5)
            throw new RuntimeException("Usage: Manager <id> <docsCapacity> <bucket> <mwQueue> <wmQueue>");
        Worker worker = new Worker(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2], args[3], args[4]);
        worker.run();
    }

    public void run() {
        System.out.println("Worker running");
        while (docsWorkedOn < docsCapacity) {
            System.out.println("Works on " + docsWorkedOn + " out of " + docsCapacity);
            Message message = sqsOperations.receiveMessage(mwQueue);
            if (message == null) {
                break;
            }

            sqsOperations.changeVisibilityTimeout(mwQueue, message.receiptHandle(), 90);
            String body = message.body();

            try {
                String[] parts = body.split("\t");
                String action = parts[0];
                String pdfLoc = parts[1];

                String outputLoc = process(action, pdfLoc, message.receiptHandle());
                String response = action + "\t" + pdfLoc + "\t" + outputLoc;
                sqsOperations.sendMessage(wmQueue, response);
            }
            catch (Exception e) {
                System.err.println("Failed to process message: " + message.body());
                e.printStackTrace();
                sqsOperations.sendMessage(wmQueue, body + "\t" + e.getMessage());
            }

            sqsOperations.deleteMessage(mwQueue, message.receiptHandle());
            docsWorkedOn++;
        }

        sqsOperations.sendMessage(wmQueue, "terminate\t" + id);
    }

    public String process(String action, String pdfLoc, String messageReceiptHandle) {
        try {
            File pdfFile = downloadPDF(pdfLoc, messageReceiptHandle);
            PDDocument document = Loader.loadPDF(pdfFile);

            String outputLoc = null;
            assert action != null;
            if (action.equals("ToImage"))
                outputLoc = extractImagesFromPDF(document, pdfLoc);
            else if (action.equals("ToHTML"))
                outputLoc = convertPDFToHTML(document, pdfLoc);
            else if (action.equals("ToText"))
                outputLoc = extractTextFromPDF(document, pdfLoc);
            else
                System.out.println("Invalid action: " + action);

            document.close();

            if (outputLoc == null) {
                System.err.println("Failed to process pdf: " + pdfLoc);
                return "Failed to process pdf: " + pdfLoc;
            }

            return outputLoc;
        } catch (Exception e) {
            System.err.println("Failed to process pdf: " + pdfLoc);
            return e.getMessage();
        }
    }

    private File downloadPDF(String pdfUrl, String messageReceiptHandle) throws IOException {
        URL url = new URL(pdfUrl);
        File tempFile = Files.createTempFile("downloaded_pdf", ".pdf").toFile();

        try (InputStream inputStream = url.openStream();
             OutputStream outputStream = new FileOutputStream(tempFile)) {

            long startTime = System.currentTimeMillis();
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                if (System.currentTimeMillis() - startTime > 10 * 1000) {
                    sqsOperations.changeVisibilityTimeout(mwQueue, messageReceiptHandle, 30);
                    startTime = System.currentTimeMillis();
                }
                outputStream.write(buffer, 0, bytesRead);
            }
        }

        return tempFile;
    }

    public String extractImagesFromPDF(PDDocument document, String outputFilePath) throws IOException {
        PDFRenderer renderer = new PDFRenderer(document);

        String outputImagePath = outputFilePath + ".png";
        BufferedImage image = renderer.renderImageWithDPI(0, 300);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, "PNG", baos);
        byte[] imageBytes = baos.toByteArray();

        String s3FileName = s3Operations.uploadBytesAsFile(bucket, outputImagePath, imageBytes);
        System.out.println("[ToImage] " + s3FileName + ": Succeeded");

        return s3FileName;
    }

    private String extractTextFromPDF(PDDocument document, String outputFilePath) throws IOException {
        PDFTextStripper pdfStripper = new PDFTextStripper();
        pdfStripper.setSortByPosition(true); // Optional: Keeps the text in reading order
        String content = pdfStripper.getText(document);

        // Write content to a file
        String outputTextPath = outputFilePath + ".txt";
        //writeToFile(content, "processed_" + outputTextPath);
        String s3FileName = s3Operations.uploadContentAsFile(bucket, outputTextPath, content);
        System.out.println("[ToText] " + s3FileName + ": Succeeded");

        return s3FileName;
    }

    private String convertPDFToHTML(PDDocument document, String outputFilePath) throws IOException {
        PDFTextStripper pdfStripper = new PDFTextStripper();
        pdfStripper.setSortByPosition(true); // Optional: Keeps the text in reading order
        String content = pdfStripper.getText(document);

        String outputHtmlPath = outputFilePath + ".html";
        //writeToFile("<html><body><pre>" + content + "</pre></body></html>", "processed_" + outputHtmlPath);
        String s3FileName = s3Operations.uploadContentAsFile(bucket, outputHtmlPath, "<html><body><pre>" + content + "</pre></body></html>");
        System.out.println("[ToHTML] " + s3FileName + ": Succeeded");

        return s3FileName;
    }
}
