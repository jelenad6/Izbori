package election;

public class SubmitResultCommand {

    private final int electionId;
    private final int pollingStationId;
    private final String controllerId;
    private final int totalVoters;
    private final int invalidVotes;
    private final String votesData;

    public SubmitResultCommand(
            int electionId,
            int pollingStationId,
            String controllerId,
            int totalVoters,
            int invalidVotes,
            String votesData
    ) {
        this.electionId = electionId;
        this.pollingStationId = pollingStationId;
        this.controllerId = controllerId;
        this.totalVoters = totalVoters;
        this.invalidVotes = invalidVotes;
        this.votesData = votesData;
    }

    public String writeToString() {
        return "SUBMIT_RESULT" + " "
                + electionId + " "
                + pollingStationId + " "
                + controllerId + " "
                + totalVoters + " "
                + invalidVotes + " "
                + votesData;
    }

    public int getElectionId() {
        return electionId;
    }

    public int getPollingStationId() {
        return pollingStationId;
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

    public String getVotesData() {
        return votesData;
    }
}