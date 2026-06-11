package election;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.zookeeper.KeeperException;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import rs.raf.pds.faulttolerance.core.ReplicaNode;

public class ElectionAppServer implements ReplicaNode.LogCommandExecutor {

    public static final String APP_ROOT_NODE = "/election";
    private final String snapshotFileName = "election.snapshot"; //novo za snapshot
    private final String snapshotIndexFileName = "election.snapshot.index"; // da log krece od snapshota
    private long snapshotIndex = 0;
    private boolean recovering = false;
    private long lastLogUpdateTime = System.currentTimeMillis(); // za sync

    public enum ExecutionStatus {
        STATUS_OK,
        INTERNAL_ERROR,
        LOG_ERROR,
        UPDATE_REJECTED_NOT_LEADER,
        INVALID_VOTE_COUNT,
        POLLING_STATION_BLOCKED
    }

    public record OperationStatus(
            int requestId,
            ExecutionStatus status
    ) implements Serializable {}

    ReplicaNode myReplicaNode = null;

    ElectionServiceImpl electionService;

    public ElectionAppServer(
            ElectionServiceImpl electionService,
            String zkAddress,
            String zkRoot,
            String myGRPCAddress,
            String logFileName
    ) throws FileNotFoundException {

        this.electionService = electionService;

        this.myReplicaNode =
                new ReplicaNode(
                        zkAddress,
                        zkRoot,
                        myGRPCAddress,
                        logFileName,
                        this
                );
        	loadSnapshot(); // novo za snapshot
        	recoverFromLog(); //novo za log
    }

    protected ReplicaNode getReplicaNode() {
        return myReplicaNode;
    }
    
    public ElectionServiceImpl getElectionService() {
        return electionService;
    }
    
    private void recoverFromLog() { //novo za log

        try {

            recovering = true;

            var logEntries =
                    myReplicaNode
                            .getReplicatedLog()
                            .readAllLogEntries();

            System.out.println("Recovery started...");

            for (int i = 0; i < logEntries.size(); i++) { //uslov za snapshot

                long currentLogIndex = i + 1;

                if (currentLogIndex <= snapshotIndex) {
                    continue;
                }

                String entry = logEntries.get(i);

                if (entry == null || entry.isBlank()) {
                    continue;
                }

              /*  System.out.println(
                        "Replaying log: " + entry
                );*/

                executeReplicatedLogCommand(
                        entry.getBytes()
                );
            }

            recovering = false;

            System.out.println("Recovery finished.");

        } catch (Exception e) {

            System.out.println("Recovery error: " + e.getMessage());

            e.printStackTrace();
        }
    }
    
    private void saveSnapshot() { // novo, za snapshot

        try (
                FileOutputStream fos =
                        new FileOutputStream(snapshotFileName);

                ObjectOutputStream oos =
                        new ObjectOutputStream(fos)
        ) {

            oos.writeObject(electionService);
            
            snapshotIndex = myReplicaNode // da se vraca log od snapshota
                    .getReplicatedLog()
                    .getLastLogEntryIndex();

            try (FileOutputStream indexFos =
                         new FileOutputStream(snapshotIndexFileName)) {

                indexFos.write(
                        String.valueOf(snapshotIndex).getBytes()
                );
            }

            System.out.println("Snapshot saved.");

        } catch (Exception e) {

            System.out.println(
                    "Snapshot save error: " + e.getMessage()
            );
        }
    }
    
    private void loadSnapshot() { // novo za snapshot

        try (
                FileInputStream fis =
                        new FileInputStream(snapshotFileName);

                ObjectInputStream ois =
                        new ObjectInputStream(fis)
        ) {

            electionService =
                    (ElectionServiceImpl) ois.readObject();

            System.out.println("Snapshot loaded.");
            
            try { // za load snapshota, ne ceo log
                String indexText =
                        new String(
                                java.nio.file.Files.readAllBytes(
                                        java.nio.file.Path.of(snapshotIndexFileName)
                                )
                        );

                snapshotIndex = Long.parseLong(indexText.trim());

                System.out.println("Snapshot index loaded: " + snapshotIndex);

            } catch (Exception e) {
                snapshotIndex = 0;
            }

        } catch (Exception e) {

            System.out.println(
                    "No snapshot found."
            );
        }
    }

    @Override
    public void executeReplicatedLogCommand(byte[] commandBytes) {

        Scanner sc = new Scanner(new String(commandBytes));

        String commandType = sc.next();

        if ("SUBMIT_RESULT".equals(commandType)) {

            int electionId = sc.nextInt();
            int pollingStationId = sc.nextInt();
            String controllerId = sc.next();
            int totalVoters = sc.nextInt();
            int invalidVotes = sc.nextInt();

            String votesData = sc.next();

            submitResult(
                    -1,
                    electionId,
                    pollingStationId,
                    controllerId,
                    totalVoters,
                    invalidVotes,
                    parseVotes(votesData),
                    false
            );
            
            lastLogUpdateTime = System.currentTimeMillis(); // za sync
        }

        sc.close();
    }

    public OperationStatus submitResult(
            int requestId,
            int electionId,
            int pollingStationId,
            String controllerId,
            int totalVoters,
            int invalidVotes,
            Map<String, Integer> candidateVotes,
            boolean asLeaderToExecute
    ) {

        if (asLeaderToExecute && !myReplicaNode.isLeader()) {
            return new OperationStatus(
                    requestId,
                    ExecutionStatus.UPDATE_REJECTED_NOT_LEADER
            );
        }

        if (asLeaderToExecute) {

            String votesData = votesToString(candidateVotes);

            SubmitResultCommand command =
                    new SubmitResultCommand(
                            electionId,
                            pollingStationId,
                            controllerId,
                            totalVoters,
                            invalidVotes,
                            votesData
                    );

            try {
                myReplicaNode
                        .getReplicatedLog()
                        .appendAndReplicate(
                                command.writeToString().getBytes()
                        );

            } catch (IOException e) {
                e.printStackTrace();
                

                return new OperationStatus(
                        requestId,
                        ExecutionStatus.LOG_ERROR
                );
            }
        }

        boolean success; //dodat try catch block da se razlikuje blocked i invalid vote count

        try {

            success =
                    electionService.submitResult(
                            electionId,
                            pollingStationId,
                            controllerId,
                            totalVoters,
                            invalidVotes,
                            candidateVotes
                    );

        } catch (RuntimeException e) {

            if ("POLLING_STATION_BLOCKED".equals(e.getMessage())) {

                return new OperationStatus(
                        requestId,
                        ExecutionStatus.POLLING_STATION_BLOCKED
                );
            }

            throw e;
        }

        if (!success) {
            return new OperationStatus(
                    requestId,
                    ExecutionStatus.INVALID_VOTE_COUNT
            );
        }
        
        long currentIndex = // uslov za snapshot, da periodicno cuva 5,10,15
                myReplicaNode
                        .getReplicatedLog()
                        .getLastLogEntryIndex();

        if (!recovering &&
                currentIndex > 0 &&
                currentIndex % 5 == 0) {

            saveSnapshot();
        }

        return new OperationStatus(
                requestId,
                ExecutionStatus.STATUS_OK
        );
    }

    private String votesToString(Map<String, Integer> votes) {

        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Integer> entry : votes.entrySet()) {

            sb.append(entry.getKey())
              .append(":")
              .append(entry.getValue())
              .append(",");
        }

        return sb.toString();
    }

    private Map<String, Integer> parseVotes(String votesData) {

        Map<String, Integer> votes = new HashMap<>();

        String[] pairs = votesData.split(",");

        for (String pair : pairs) {

            if (pair.isBlank()) {
                continue;
            }

            String[] tokens = pair.split(":");

            votes.put(
                    tokens[0],
                    Integer.parseInt(tokens[1])
            );
        }

        return votes;
    }
    
    public boolean isRecentlyUpdated() { //sync
        long now = System.currentTimeMillis();
        return now - lastLogUpdateTime < 10000;
    }

    public static void main(String[] args)
            throws IOException, InterruptedException {

    	Scanner input = new Scanner(System.in);

    	String zkConnectionString = "localhost:2181";

    	System.out.print("gRPC port: ");
    	int gRPCPort = Integer.parseInt(input.nextLine());

    	System.out.print("Log file name: ");
    	String logFileName = input.nextLine();

        String myGRPCaddress =
                InetAddress.getLocalHost().getHostName()
                        + ":" + gRPCPort;

        ElectionServiceImpl electionService =
                new ElectionServiceImpl();

        ElectionAppServer appServer =
                new ElectionAppServer(
                        electionService,
                        zkConnectionString,
                        APP_ROOT_NODE,
                        myGRPCaddress,
                        logFileName
                );

        Server gRPCServer =
                ServerBuilder
                        .forPort(gRPCPort)
                        .addService(
                                new ElectionServiceGRPCServer(appServer)
                        )
                        .addService(appServer.getReplicaNode())
                        .build();

        gRPCServer.start();

        try {

            appServer.getReplicaNode().leaderElection();

            appServer.getReplicaNode().start();

            gRPCServer.awaitTermination();

            appServer.getReplicaNode().stop();

        } catch (KeeperException e) {

        } catch (InterruptedException e) {

        }
    }
}