package operations;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;
import java.util.List;

public class EC2Operations {

    private final Ec2Client ec2Client;

//    private final String IMAGE_AMI = "ami-048b561e08a546159"; // Ubuntu with Java 8
    private final String IMAGE_AMI = "ami-00e95a9222311e8ed";
    private final InstanceType INSTANCE_TYPE = InstanceType.T2_MICRO;

    public EC2Operations(Region region) {
        this.ec2Client = Ec2Client.builder().region(region).build();
    }

    // Instance operations

    public String[] runInstances(String script, int min, int max, List<Tag> tags) {
        TagSpecification tagSpec = TagSpecification.builder()
                .resourceType(ResourceType.INSTANCE)
                .tags(tags)
                .build();

        return ec2Client.runInstances(
                builder -> builder.imageId(IMAGE_AMI)
                        .instanceType(INSTANCE_TYPE)
                        .minCount(min)
                        .maxCount(max)
                        .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                        .tagSpecifications(tagSpec)
                        .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                        .keyName("vockey")
        ).instances().stream().map(Instance::instanceId).toArray(String[]::new);
    }

    public void terminateInstance(String instanceId) {
        ec2Client.terminateInstances(
                builder -> builder.instanceIds(instanceId)
        );
    }

    public String[] getAllInstances() {
        return ec2Client.describeInstances().reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .map(Instance::instanceId)
                .toArray(String[]::new);
    }

    public String[] getAllInstancesWithTag(String key, String value) {
        return ec2Client.describeInstances().reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .filter(instance -> instance.tags().stream().anyMatch(tag -> tag.key().equals(key) && tag.value().equals(value)))
                .map(Instance::instanceId)
                .toArray(String[]::new);
    }

    public Instance[] getAllInstanceObjects() {
        return ec2Client.describeInstances().reservations().stream()
                .flatMap(reservation -> reservation.instances().stream()).toArray(Instance[]::new);
    }
}
