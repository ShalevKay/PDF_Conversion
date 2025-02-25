# Distributed System Programing: Assignment 1

### Noam Argaman: 322985979
### Shalev Kayat: 211616701

##  Table of Contents
- [Running the Project](#Running the Project)
- [How the Project Works](#How the Project Works)
- [Resources](#Resources)
- [Results](#Results)
- [Mandatory Requirements](#Mandatory Requirements)

## Running the Project
- To run the project, run the following command in terminal:
   ```
      java -jar LocalApplication.jar resources/input-sample-i.txt outputfile n [terminate]
- 'i' is a number: 1, 2, according to the given examples from the moodle. Any legal input file can be used.
- 'n' is the number of docs per worker.
- "terminate" is used to indicate whether to terminate the manager or not upon finishing the work.

- The manager file is compiled using 'Manager.java' and 'LocalAppData.java' files.
- The worker file is compiled using 'Worker.java' file.
- The local application file is compiled using 'LocalApplication.java' file.
- All those files require the following files: 'EC2Operations.java', 'S3Operations.java', 'SQSOperations.java'.

## How the Project Works
- The project contains 3 sub projects, each sub project is a component of the project - LocalApplication, Manager and Worker.
1. Local Application:
   - The program starts from LocalApplication, after running the command which described above.
   - After parsing the command line arguments, the local application is created with its own 2 way SQS queue - one to send messages to the manager and one to receive messages from the manager.
   - Each local app registers to a register bucket, so the manager can get access to each local app independently.
   - The local app uploads its given input file to the localApp-manager queue, and then trys to activate the manager, in case the manager is not running yet.
   - Then, the local app wait for the work to be done, and after receiving the finishing message, it downloads the summery file from s3, terminate the manager in case it's indicated int the command line arguments.
   - Then, the local app waits for the manager to tell it if it has been successfully terminated.
   - At last, the local app deletes its bucket and the run is terminated.
2. Manager:
    - The Manager is started by a local app, creating 2 SQS queues to send messages to workers and receive messages from workers.
    - The Manager has a fixed thread pool for executing with multiple clients, and a list for all registered local applications.
    - The Manager parses the command line arguments which consist the required information by the local app.
    - The Manager starts running, and registers any local app which tried to activate him, and submits its messages to the thread pool.
    - The Manager gets messages from a queue and send it to be processed.
    - The first message is the file of pdf. The Manager reads its lines, and then, according to the given 'n' from the local app, it's starting to deploy workers to work on processing the files, with the limitation of 8 workers.
    - After all the pdfs are processed, the Manager creates a summery file, upload it to s3, and send a message to the local app-manager queue.
    - Then, the Manager is shutting down, if it was asked in the local app command line arguments.
3. Worker:
    - The worker is started by the manager, and is designated to perform a certain amount of tasks.
    - For each task, it downloads the file from the internet
    - Then for each task, it processes each file according to their specified type of task (ToImage, ToText, ToHTML).
    - Then for each task, it uploads the processed file back to the cloud and sends a message to the manager that the task has been completed.
    - The worker is terminated by the manager after all tasks have been completed.
   
## Resources
- Instance used: IMAGE_AMI = "ami-00e95a9222311e8ed"
- Instance type: micro

## Results
- To run our program, we used n = 50 for both input files
1. Time took to run the program:
   - For input-sample-1.txt: 493.665 seconds
   - For input-sample-2.txt: 149.406 seconds
2. Security:
   - For security, we used the given credentials in the AWS lab, written in a file named "credentials" under the .aws folder in the user folder. This was explained by the TA.
   - Any time we needed to activate the AWS lab, the first thing we did was to replace the old credentials with the new ones, and even if we forgot, there was an exception to remind us that the credentials are not valid anymore.
3. Scalability:
   - The program is scalable since we implemented some scalability features:
   - The workers work in parallel, since each one is on a different computer sending their results back to the main manager.
   - The manager has a thread pool, so it can take many different tasks and distribute them in parallel to the different threads that manager has. Currently, using minimal resources the process will be slow with many users, but the program is made to be scalable with just increasing the amount of resources.
4. Persistence: 
   - The program is persistent since we used the full features of the SQS client: Every message has a timeout, and during the process it increases the timeout until we finish processing the message - then we delete it.
   - If during the process the worker fails, than the message will reappear to other workers and everything will eventually be processed.
5. Threads:
   - We invested some time thinking about where threads can help us.
   - We settled on using them in the manager, since the manager is the one that needs to handle multiple clients and multiple workers.
   - We used a thread pool to handle multiple clients at the same time.
   - However, in the worker we didn't think we needed threads because using them can have large overhead and each worker only requires doing minimal tasks (that are not scalable).
6. Multiple clients:
   - We tested two clients at the same time, and the program worked as expected.
   - The users got their results (and only their results) and the manager successfully managed to send them their final correct results.
7. Process termination:
   - We implemented the process termination feature, and it worked as expected.
   - Firstly, if the local app requests the manager to shut down, the manager will first complete its tasks and only then shut down.
   - Secondly, the workers when they finished working didn't shut down themselves, but the manager that created them does. This is better as it allows the manager to complete gathering all the results from the queues of the workers before them being shut down.
8. Limitations:
   - We minded the limitations of the AWS.
   - We made sure that no more than 9 instances of ec2 are deployd, with a constant field that equals 8 - the maximal workers number, and another one for the manager.
9. Workers' work:
   - The requirements indicate that each worker needs to process 'n' files, so the system is built in a way to make sure that each worker processes exactly 'n' files.
   - We used visibility timeout to make sure that each pdf file is processed only by one worker.
10. The manager does what it needs to do:
    - The manager's work in the system, is to receive messages from local applications, and deploy workers on AWS computers to process the given input files or shut them down.
    - The manager manages the system and makes sure the workers are running properly, and send or receive messages to them.
11. Distributed System: 
   - The system is distributed.
   - The system does not run locally, but only with the local application as needed.
   - The system uses distributed computers of the AWS to process the given input file, or send messages through the system components and saving files on s3.
