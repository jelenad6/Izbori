package election;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import rs.raf.pds.faulttolerance.core.ReplicaNode;
import rs.raf.pds.faulttolerance.core.SyncPrimitive;
import rs.raf.pds.faulttolerance.gRPC.CandidateVote;
import rs.raf.pds.faulttolerance.gRPC.ElectionResponse;
import rs.raf.pds.faulttolerance.gRPC.ElectionServiceGrpc;
import rs.raf.pds.faulttolerance.gRPC.RequestStatus;
import rs.raf.pds.faulttolerance.gRPC.VotingResultRequest;
import rs.raf.pds.faulttolerance.gRPC.StatisticsRequest;
import rs.raf.pds.faulttolerance.gRPC.StatisticsResponse;

public class ElectionClient extends SyncPrimitive {

    final String appRoot;

    String leaderNodeName = null;
    String leaderHostNamePort;

    Object zkNotifier = new Object();

    ManagedChannel channel = null;

    ElectionServiceGrpc.ElectionServiceBlockingStub blockingStub = null;

    protected ElectionClient(String zkAddress, String appRoot)
            throws KeeperException, InterruptedException {
        super(zkAddress);
        this.appRoot = appRoot;
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Stigla notifikacija od ZooKeepera!");

        synchronized (zkNotifier) {
            zkNotifier.notify();
        }
    }

    protected void newLeaderAwaiting()
            throws KeeperException, InterruptedException {

        System.out.println("Trazenje novog lidera!");

        synchronized (zkNotifier) {
            zkNotifier.wait();
        }

        checkLeader();
    }

    public synchronized void checkLeader()
            throws KeeperException, InterruptedException {

        List<String> list = zk.getChildren(appRoot, false);

        System.out.println("There are total: " + list.size() + " replicas for elections!");

        for (String node : list) {
            System.out.print("NODE: " + node + ", ");
        }

        System.out.println();

        if (list.size() == 0) {
            System.out.println("Nema aktivnih replika.");
            return;
        }

        Integer minValue =
                Integer.parseInt(
                        list.get(0).substring(
                                ReplicaNode.REPLICA_NODE_SEQUENCE_INDEX
                        )
                );

        String minNodeName = list.get(0);

        for (int i = 1; i < list.size(); i++) {

            Integer tempValue =
                    Integer.parseInt(
                            list.get(i).substring(
                                    ReplicaNode.REPLICA_NODE_SEQUENCE_INDEX
                            )
                    );

            if (minValue > tempValue) {
                minValue = tempValue;
                minNodeName = list.get(i);
            }
        }

        if (leaderNodeName == null || !minNodeName.equals(leaderNodeName)) {

            leaderNodeName = minNodeName;

            byte[] b = zk.getData(
                    appRoot + "/" + leaderNodeName,
                    true,
                    null
            );

            leaderHostNamePort = new String(b);

            System.out.println("Leader je " + leaderNodeName);
            System.out.println("Leader address: " + leaderHostNamePort);

            blockingStub = getBlockingStub(leaderHostNamePort);
        }
    }

    public ElectionServiceGrpc.ElectionServiceBlockingStub getBlockingStub(
            String hostNamePort
    ) {
        String[] splits = hostNamePort.split(":");

        channel =
                ManagedChannelBuilder
                        .forAddress(
                                splits[0],
                                Integer.parseInt(splits[1])
                        )
                        .usePlaintext()
                        .build();

        return ElectionServiceGrpc.newBlockingStub(channel);
    }

    private void inviteServer()
            throws KeeperException, InterruptedException {

        try {

           // sendSubmitResultRequest();

           // Thread.sleep(200);

           // sendSecondControllerResult();
            
            simulateConcurrentControllers(); //za kontrolera

            Thread.sleep(2000); //za kontrolera
            
            
            
            simulateElection(1, 601, "ctrl1", "ctrl2", List.of("ListaA", "ListaB", "ListaC"));

            simulateElection(2, 701, "ctrl3", "ctrl4", List.of("Kandidat1", "Kandidat2"));

            simulateElection(3, 801, "ctrl5", "ctrl6", List.of("LokalnaLista1", "LokalnaLista2"));

            getStatistics(1);
            getStatistics(2);
            getStatistics(3);
            
            
          /*  testFollowerStatistics( // za sinhronizaciju
                    "localhost:5002"
            );*/
            
          //  testBlockedPollingStation(); //test blocked, samo kad se testira blocked metoda!

        } catch (RuntimeException e) {

            System.out.println(
                    "ERROR - Server has crashed! Exception: "
                            + e.getMessage()
            );

            newLeaderAwaiting();

        } finally {

            if (channel != null) {
                channel.shutdown();
            }
        }
    }

    private void sendSubmitResultRequest() {

        System.out.println("Saljem rezultat prvog kontrolora...");

        VotingResultRequest request =
                VotingResultRequest.newBuilder()
                        .setRequestId(1)
                        .setElectionId(1)
                        .setPollingStationId(101)
                        .setControllerId("kontrolor1")
                        .setTotalVoters(100)
                        .setInvalidVotes(10)
                        .addCandidateVotes(
                                CandidateVote.newBuilder()
                                        .setCandidateName("ListaA")
                                        .setVotes(60)
                                        .build()
                        )
                        .addCandidateVotes(
                                CandidateVote.newBuilder()
                                        .setCandidateName("ListaB")
                                        .setVotes(30)
                                        .build()
                        )
                        .build();

        ElectionResponse response = blockingStub.submitResult(request);

        printResponse(response);
    }
    

    private void sendSecondControllerResult() {

        System.out.println("Saljem rezultat drugog kontrolora...");

        VotingResultRequest request =
                VotingResultRequest.newBuilder()
                        .setRequestId(2)
                        .setElectionId(1)
                        .setPollingStationId(101)
                        .setControllerId("kontrolor2")
                        .setTotalVoters(100)
                        .setInvalidVotes(10)
                        .addCandidateVotes(
                                CandidateVote.newBuilder()
                                        .setCandidateName("ListaA")
                                        .setVotes(60)
                                        .build()
                        )
                        .addCandidateVotes(
                                CandidateVote.newBuilder()
                                        .setCandidateName("ListaB")
                                        .setVotes(30)
                                        .build()
                        )
                        .build();

        ElectionResponse response = blockingStub.submitResult(request);

        printResponse(response);
    }
    
    private void getStatistics(
            int electionId
    ) {

        StatisticsRequest request =
                StatisticsRequest.newBuilder()
                        .setRequestId(
                                100 + electionId
                        )
                        .setElectionId(
                                electionId
                        )
                        .build();

        StatisticsResponse response =
                blockingStub.getStatistics(
                        request
                );

        System.out.println();

        System.out.println(
                "STATISTICS FOR ELECTION "
                + electionId
        );

        System.out.println(
                "Election type: "
                + response.getElectionType()
        );

        System.out.println(
                "Processed polling stations: "
                + response
                        .getProcessedPollingStationsPercentage()
        );

        System.out.println(
                "Repeat polling stations: "
                + response
                        .getRepeatPollingStations()
        );

        System.out.println(
                "Turnout percentage: "
                + response
                        .getTurnoutPercentage()
        );

        System.out.println(
                "Candidate results:"
        );

        response
                .getCandidateStatisticsList()
                .forEach(candidate -> {

                    System.out.println(
                            candidate.getCandidateName()
                            + ": "
                            + candidate.getTotalVotes()
                    );

                });
    }
    
    

    private static void printResponse(ElectionResponse response) {

        if (response.getStatus() == RequestStatus.STATUS_OK) {
            System.out.println("STATUS OK: " + response.getMessage());
        } else {
            System.out.println(
                    "STATUS ERROR: "
                            + response.getStatus()
                            + " - "
                            + response.getMessage()
            );
        }
    }
    
    private void testBlockedPollingStation() { //test blocked

        System.out.println("TEST BLOCKED POLLING STATION");

        sendCustomResult(400, "kontrolorA", 60, 30);
        sendCustomResult(400, "kontrolorB", 50, 40);

        sendCustomResult(400, "kontrolorC", 55, 35);
        sendCustomResult(400, "kontrolorD", 45, 45);

        sendCustomResult(400, "kontrolorE", 60, 30);
    }
    
    private void sendCustomResult( //helper funkcija za testiranje blokiranja
            int pollingStationId,
            String controllerId,
            int listaAVotes,
            int listaBVotes
    ) {
        VotingResultRequest request =
                VotingResultRequest.newBuilder()
                        .setRequestId((int) System.currentTimeMillis())
                        .setElectionId(1)
                        .setPollingStationId(pollingStationId)
                        .setControllerId(controllerId)
                        .setTotalVoters(100)
                        .setInvalidVotes(10)
                        .addCandidateVotes(
                                CandidateVote.newBuilder()
                                        .setCandidateName("ListaA")
                                        .setVotes(listaAVotes)
                                        .build()
                        )
                        .addCandidateVotes(
                                CandidateVote.newBuilder()
                                        .setCandidateName("ListaB")
                                        .setVotes(listaBVotes)
                                        .build()
                        )
                        .build();

        ElectionResponse response = blockingStub.submitResult(request);

        System.out.println(
                controllerId + " → " + response.getStatus()
                        + " / " + response.getMessage()
        );
    }
    
    private void simulateConcurrentControllers() { //kontroleri simulacija

        System.out.println("CONCURRENT CONTROLLERS TEST");

        for (int stationId = 500; stationId < 505; stationId++) {

            final int currentStationId = stationId;

            Thread controller1 = new Thread(() -> {
                sendCustomResult(
                        currentStationId,
                        "controllerA-" + currentStationId,
                        60,
                        30
                );
            });

            Thread controller2 = new Thread(() -> {
                sendCustomResult(
                        currentStationId,
                        "controllerB-" + currentStationId,
                        60,
                        30
                );
            });

            controller1.start();
            controller2.start();
        }
    }
    
    private void testFollowerStatistics( // za sinhronizaciju
            String followerAddress
    ) {

        System.out.println(
                "FOLLOWER STATISTICS TEST"
        );

        ManagedChannel followerChannel =
                ManagedChannelBuilder
                        .forTarget(followerAddress)
                        .usePlaintext()
                        .build();

        ElectionServiceGrpc
                .ElectionServiceBlockingStub followerStub =
                ElectionServiceGrpc
                        .newBlockingStub(
                                followerChannel
                        );

        StatisticsResponse response =
                followerStub.getStatistics(
                        StatisticsRequest
                                .newBuilder()
                                .setRequestId(9999)
                                .build()
                );

        System.out.println(
                "Follower processed polling stations: "
                + response
                    .getProcessedPollingStationsPercentage()
        );

        System.out.println(
                "Follower turnout: "
                + response
                    .getTurnoutPercentage()
        );

        followerChannel.shutdown();
    }
    
    private void sendResultForElection(
            int electionId,
            int pollingStationId,
            String controllerId,
            Map<String,Integer> votes
    ) {

        VotingResultRequest.Builder builder =
                VotingResultRequest.newBuilder()
                        .setRequestId(
                                (int)System.currentTimeMillis()
                        )
                        .setElectionId(electionId)
                        .setPollingStationId(
                                pollingStationId
                        )
                        .setControllerId(controllerId)
                        .setTotalVoters(100)
                        .setInvalidVotes(10);

        for(
                Map.Entry<String,Integer> entry
                : votes.entrySet()
        ){

            builder.addCandidateVotes(
                    CandidateVote
                            .newBuilder()
                            .setCandidateName(
                                    entry.getKey()
                            )
                            .setVotes(
                                    entry.getValue()
                            )
                            .build()
            );
        }

        ElectionResponse response =
                blockingStub.submitResult(
                        builder.build()
                );

        System.out.println(
                "Election "
                + electionId
                + " / "
                + controllerId
                + " → "
                + response.getStatus()
        );
    }
    
    private void simulateElection( // simulacija izbora
            int electionId,
            int pollingStationId,
            String controller1,
            String controller2,
            List<String> candidates
    ) {

        Map<String,Integer> votes =
                new HashMap<>();

        int validVotes = 90;

        int remaining = validVotes;

        Random random = new Random();

        for(int i=0;i<candidates.size();i++){

            int candidateVotes;

            if(i == candidates.size()-1){

                candidateVotes = remaining;

            } else {

                candidateVotes =
                        random.nextInt(
                                remaining+1
                        );

                remaining -= candidateVotes;
            }

            votes.put(
                    candidates.get(i),
                    candidateVotes
            );
        }

        sendResultForElection(
                electionId,
                pollingStationId,
                controller1,
                votes
        );

        sendResultForElection(
                electionId,
                pollingStationId,
                controller2,
                votes
        );
    }
    

    public static void main(String[] args) {

    	String zkConnectionString = "localhost:2181";

    	try {

    	    ElectionClient client =
    	            new ElectionClient(
    	                    zkConnectionString,
    	                    ElectionAppServer.APP_ROOT_NODE
    	            );

            client.checkLeader();

            client.inviteServer();

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}