package election;

import java.util.HashMap;
import java.util.Map;

import io.grpc.stub.StreamObserver;

import rs.raf.pds.faulttolerance.gRPC.CandidateVote;
import rs.raf.pds.faulttolerance.gRPC.ElectionResponse;
import rs.raf.pds.faulttolerance.gRPC.ElectionServiceGrpc.ElectionServiceImplBase;
import rs.raf.pds.faulttolerance.gRPC.RequestStatus;
import rs.raf.pds.faulttolerance.gRPC.StatisticsRequest;
import rs.raf.pds.faulttolerance.gRPC.StatisticsResponse;
import rs.raf.pds.faulttolerance.gRPC.VotingResultRequest;
import rs.raf.pds.faulttolerance.gRPC.CandidateStatistics;

public class ElectionServiceGRPCServer extends ElectionServiceImplBase {

    final ElectionAppServer appReplicatedServer;

    public ElectionServiceGRPCServer(ElectionAppServer server) {
        this.appReplicatedServer = server;
    }

    @Override
    public void submitResult(
            VotingResultRequest request,
            StreamObserver<ElectionResponse> responseObserver
    ) {
        Map<String, Integer> candidateVotes = new HashMap<>();

        for (CandidateVote vote : request.getCandidateVotesList()) {
            candidateVotes.put(vote.getCandidateName(), vote.getVotes());
        }

        ElectionAppServer.OperationStatus opStatus =
                appReplicatedServer.submitResult(
                        request.getRequestId(),
                        request.getElectionId(),
                        request.getPollingStationId(),
                        request.getControllerId(),
                        request.getTotalVoters(),
                        request.getInvalidVotes(),
                        candidateVotes,
                        true
                );

        ElectionResponse response;

        if (ElectionAppServer.ExecutionStatus.UPDATE_REJECTED_NOT_LEADER == opStatus.status()) {

            response = ElectionResponse.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setStatus(RequestStatus.UPDATE_REJECTED_NOT_LEADER)
                    .setMessage("This server is not leader.")
                    .build();

        } else if (ElectionAppServer.ExecutionStatus.INVALID_VOTE_COUNT == opStatus.status()) {

            response = ElectionResponse.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setStatus(RequestStatus.INVALID_VOTE_COUNT)
                    .setMessage("Candidate votes do not match valid votes.")
                    .build();

        } else if (ElectionAppServer.ExecutionStatus.STATUS_OK == opStatus.status()) {

            response = ElectionResponse.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setStatus(RequestStatus.STATUS_OK)
                    .setMessage("Voting result accepted.")
                    .build();

        }  
        else if (ElectionAppServer.ExecutionStatus.POLLING_STATION_BLOCKED == opStatus.status()) { //za blocked

            response = ElectionResponse.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setStatus(RequestStatus.POLLING_STATION_BLOCKED)
                    .setMessage("Polling station is blocked.")
                    .build();
        }
        else {

            response = ElectionResponse.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setStatus(RequestStatus.STATUS_OK)
                    .setMessage("Unknown status.")
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getStatistics( //statistika
            StatisticsRequest request,
            StreamObserver<StatisticsResponse> responseObserver
    ) {
    	
    	if (!appReplicatedServer.getReplicaNode().isLeader() //sync
    	        && !appReplicatedServer.isRecentlyUpdated()) {

    	    StatisticsResponse response =
    	            StatisticsResponse.newBuilder()
    	                    .setStatus(RequestStatus.UPDATE_REJECTED_NOT_LEADER)
    	                    .build();

    	    responseObserver.onNext(response);
    	    responseObserver.onCompleted();
    	    return;
    	}
    	int electionId = request.getElectionId();

        ElectionServiceImpl service =
                appReplicatedServer.getElectionService();

        StatisticsResponse.Builder responseBuilder =
                StatisticsResponse.newBuilder()
                        .setStatus(RequestStatus.STATUS_OK)

                        .setElectionId(electionId)

                        .setElectionType(
                                service
                                        .getElectionType(
                                                electionId
                                        )
                                        .name()
                        )

                        .setTurnoutPercentage(
                                service.getTurnoutPercentage(electionId)
                        )

                        .setProcessedPollingStationsPercentage(
                                service.getProcessedPollingStationsPercentage(electionId)
                        )

                        .setRepeatPollingStations(
                                service.getRepeatPollingStationsCount(electionId)
                        );

        Map<String, Integer> totals =
                service.getTotalVotesPerCandidate(electionId);

        for (Map.Entry<String, Integer> entry : totals.entrySet()) {
            responseBuilder.addCandidateStatistics(
                    CandidateStatistics.newBuilder()
                            .setCandidateName(entry.getKey())
                            .setTotalVotes(entry.getValue())
                            .build()
            );
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}