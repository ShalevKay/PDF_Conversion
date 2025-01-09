package operations;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class S3Operations {

    private final S3Client s3Client;
    private final Region region;

    public S3Operations(Region region) {
        this.s3Client = S3Client.builder().region(region).build();
        this.region = region;
    }

    // Bucket operations

    public void createBucket(String name) {
        System.out.println("Creating bucket " + name);
        s3Client.createBucket(builder -> {
            if (!region.id().equals("us-east-1")) {
                builder.bucket(name).createBucketConfiguration(
                        config -> config.locationConstraint(region.id())
                );
            } else {
                builder.bucket(name);
            }
        });
    }

    public void deleteBucket(String name) {
        System.out.println("Deleting bucket " + name);
        String[] files = listFiles(name);
        for (String file : files) {
            deleteFile(name, file);
        }

        s3Client.deleteBucket(
                builder -> builder.bucket(name)
        );
    }

    public boolean doesBucketExist(String name) {
        return s3Client.listBuckets().buckets().stream().anyMatch(
                bucket -> bucket.name().equals(name)
        );
    }

    // File operations

    public String uploadFile(String bucketName, String key, File file) {
        System.out.println("Uploading file " + file.getName() + " to bucket " + bucketName);
        s3Client.putObject(
                builder -> builder.bucket(bucketName).key(key).build(),
                file.toPath()
        );
        return "s3://" + bucketName + "/" + key;
    }

    public String uploadContentAsFile(String bucketName, String key, String content) {
        System.out.println("Uploading content to bucket " + bucketName);
        s3Client.putObject(
                builder -> builder.bucket(bucketName).key(key).build(),
                RequestBody.fromString(content)
        );
        return "s3://" + bucketName + "/" + key;
    }

    public String uploadBytesAsFile(String bucketName, String key, byte[] content) {
        System.out.println("Uploading content to bucket " + bucketName);
        s3Client.putObject(
                builder -> builder.bucket(bucketName).key(key).build(),
                RequestBody.fromBytes(content)
        );
        return "s3://" + bucketName + "/" + key;
    }

    public void downloadFile(String bucketName, String key, File file) {
        System.out.println("Downloading file " + key + " from bucket " + bucketName);

        try {
            // Ensure the parent directory exists
            Path parentDir = file.toPath().getParent();
            if (parentDir != null) {
                Files.createDirectories(parentDir);
            }

            // if file exists locally - delete it
            if (file.exists()) {
                file.delete();
            }

            s3Client.getObject(
                    builder -> builder.bucket(bucketName).key(key).build(),
                    file.toPath()
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteFile(String bucketName, String key) {
        System.out.println("Deleting file " + key + " from bucket " + bucketName);
        s3Client.deleteObject(
                builder -> builder.bucket(bucketName).key(key)
        );
    }

    public String[] listFiles(String bucketName) {
        System.out.println("Listing files in bucket " + bucketName);
        return s3Client.listObjectsV2(
                builder -> builder.bucket(bucketName).build()
        ).contents().stream().map(S3Object::key).toArray(String[]::new);
    }

    public boolean doesFileExist(String bucketName, String key) {
        String[] files = listFiles(bucketName);
        return Arrays.asList(files).contains(key);
    }
}
