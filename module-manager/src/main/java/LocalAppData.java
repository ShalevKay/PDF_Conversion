import java.util.LinkedList;
import java.util.List;

public class LocalAppData {

    private final int docsPerWorker;
    private final String lmQueue;
    private final String mlQueue;
    private final String bucket;
    private boolean running;
    private final List<String> results;

    public LocalAppData(int docsPerWorker, String lmQueue, String mlQueue, String bucket) {
        this.docsPerWorker = docsPerWorker;
        this.lmQueue = lmQueue;
        this.mlQueue = mlQueue;
        this.bucket = bucket;
        this.running = true;
        this.results = new LinkedList<>();
    }

    public int getDocsPerWorker() {
        return docsPerWorker;
    }

    public String getLmQueue() {
        return lmQueue;
    }

    public String getMlQueue() {
        return mlQueue;
    }

    public String getBucket() {
        return bucket;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        System.out.println("Setting running to " + running);
        this.running = running;
    }

    public void addResult(String result) {
        results.add(result);
    }

    public List<String> getResults() {
        return results;
    }

}
