package election;

import java.util.Map;
import java.io.Serializable;

public class ControllerResult implements Serializable {

    private final String controllerId;
    private final int totalVoters;
    private final int invalidVotes;
    private final Map<String, Integer> candidateVotes;

    public ControllerResult(
            String controllerId,
            int totalVoters,
            int invalidVotes,
            Map<String, Integer> candidateVotes
    ) {
        this.controllerId = controllerId;
        this.totalVoters = totalVoters;
        this.invalidVotes = invalidVotes;
        this.candidateVotes = candidateVotes;
    }

    public String getControllerId() {
        return controllerId;
    }

    public int getTotalVoters() {
        return totalVoters;
    }

    public int getInvalidVotes() {
        return invalidVotes;
    }

    public Map<String, Integer> getCandidateVotes() {
        return candidateVotes;
    }
}