package election;

import java.util.HashMap;
import java.util.Map;
import java.io.Serializable;

public class PollingStationResult implements Serializable {

    public enum PollingStationStatus {
        WAITING,
        VERIFIED,
        NEEDS_REPEAT,
        BLOCKED
    }

    private final int electionId;
    private final int pollingStationId;

    private PollingStationStatus status = PollingStationStatus.WAITING;
    private int failedVerificationCount = 0;

    private final Map<String, ControllerResult> controllerResults = new HashMap<>();

    public PollingStationResult(int electionId, int pollingStationId) {
        this.electionId = electionId;
        this.pollingStationId = pollingStationId;
    }

    public void addControllerResult(
            String controllerId,
            int totalVoters,
            int invalidVotes,
            Map<String, Integer> candidateVotes
    ) {
        ControllerResult result = new ControllerResult(
                controllerId,
                totalVoters,
                invalidVotes,
                candidateVotes
        );

        controllerResults.put(controllerId, result);
    }

    public int getElectionId() {
        return electionId;
    }

    public int getPollingStationId() {
        return pollingStationId;
    }

    public Map<String, ControllerResult> getControllerResults() {
        return controllerResults;
    }

    public PollingStationStatus getStatus() {
        return status;
    }

    public void setStatus(PollingStationStatus status) {
        this.status = status;
    }

    public int getFailedVerificationCount() {
        return failedVerificationCount;
    }

    public void increaseFailedVerificationCount() {
        this.failedVerificationCount++;
    }
    public void verifyResults() {

        if (controllerResults.size() < 2) {
            return;
        }

        ControllerResult firstResult = null;
        boolean allMatch = true;

        for (ControllerResult result : controllerResults.values()) {

            if (firstResult == null) {
                firstResult = result;
                continue;
            }

            if (!resultsMatch(firstResult, result)) {
                allMatch = false;
                break;
            }
        }

        if (allMatch) {
            status = PollingStationStatus.VERIFIED;
        } else {

            failedVerificationCount++;

            if (failedVerificationCount >= 2) {
                status = PollingStationStatus.BLOCKED;
            } else {
                status = PollingStationStatus.NEEDS_REPEAT;
            }
        }
    }
    private boolean resultsMatch(
            ControllerResult r1,
            ControllerResult r2
    ) {

        if (r1.getTotalVoters() != r2.getTotalVoters()) {
            return false;
        }

        if (r1.getInvalidVotes() != r2.getInvalidVotes()) {
            return false;
        }

        return r1.getCandidateVotes().equals(r2.getCandidateVotes());
    }
}