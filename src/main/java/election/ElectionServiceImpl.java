package election;

import java.util.HashMap;
import java.util.Map;
import java.io.Serializable;
import java.util.List;
import java.util.Random;

public class ElectionServiceImpl implements Serializable {
	
	public enum ElectionType {
	    PARLIAMENTARY,
	    PRESIDENTIAL,
	    LOCAL
	}
	 
    private final Map<Integer, PollingStationResult> pollingStations = new HashMap<>();
    //za 1000 mesta, kandidate, liste, upisani glasaci
    private final Map<Integer, Integer> registeredVotersByStation = new HashMap<>();
    private final Map<Integer, List<String>> electionCandidates = new HashMap<>(); //za kandidate
    private final Map<Integer, ElectionType> electionTypes = new HashMap<>(); //tip izbora
    private static final int TOTAL_POLLING_STATIONS = 1000;

    public ElectionServiceImpl() {
        initializeElectionData();
    }

    public synchronized boolean submitResult(
            int electionId,
            int pollingStationId,
            String controllerId,
            int totalVoters,
            int invalidVotes,
            Map<String, Integer> candidateVotes
    ) {
    	
    	if (!registeredVotersByStation.containsKey(pollingStationId)) {
    	    return false;
    	}

    	if (!electionCandidates.containsKey(electionId)) { //novo, za kandidate
    	    return false;
    	}

    	int registeredVoters = registeredVotersByStation.get(pollingStationId);

    	if (totalVoters > registeredVoters) {
    	    return false;
    	}
        int validVotes = totalVoters - invalidVotes;

        int sumCandidateVotes = 0;
        for (Integer votes : candidateVotes.values()) {
            sumCandidateVotes += votes;
        }

        if (sumCandidateVotes != validVotes) {
            return false;
        }

        PollingStationResult result = pollingStations.get(pollingStationId);
        
        //provera kontrolera, zabrana daljeg unosa
        if (result != null &&
                result.getStatus() == PollingStationResult.PollingStationStatus.BLOCKED) {

            throw new RuntimeException("POLLING_STATION_BLOCKED");
        }

        if (result == null) {
            result = new PollingStationResult(electionId, pollingStationId);
            pollingStations.put(pollingStationId, result);
        }

        result.addControllerResult(
                controllerId,
                totalVoters,
                invalidVotes,
                candidateVotes
        );

        result.verifyResults();

        return true;
    }
    
    public synchronized ElectionType getElectionType( //getter za tip izbora
            int electionId
    ) {

        return electionTypes.get(electionId);
    }

    public synchronized int getProcessedPollingStationsCount() {
        return pollingStations.size();
    }

    public synchronized Map<Integer, PollingStationResult> getPollingStations() {
        return pollingStations;
    }
    
    public synchronized int getRepeatPollingStationsCount(int electionId) {
        int count = 0;

        for (PollingStationResult result : pollingStations.values()) {

            if (result.getElectionId() != electionId) {
                continue;
            }

            if (result.getStatus() == PollingStationResult.PollingStationStatus.NEEDS_REPEAT) {
                count++;
            }
        }

        return count;
    }

    public synchronized int getTotalProcessedPollingStations() { //novo
        return pollingStations.size();
    }
    
    public synchronized float getProcessedPollingStationsPercentage(int electionId) {
        int count = 0;

        for (PollingStationResult result : pollingStations.values()) {

            if (result.getElectionId() != electionId) {
                continue;
            }

            count++;
        }

        return ((float) count / TOTAL_POLLING_STATIONS) * 100;
    }

    public synchronized float getTurnoutPercentage(int electionId) { //novo, za statistiku
        int totalRegistered = 0;
        int totalTurnout = 0;

        for (PollingStationResult result : pollingStations.values()) {

            if (result.getElectionId() != electionId) {
                continue;
            }

            int stationId = result.getPollingStationId();

            totalRegistered += registeredVotersByStation.getOrDefault(stationId, 0);

            for (ControllerResult controllerResult : result.getControllerResults().values()) {
                totalTurnout += controllerResult.getTotalVoters();
                break;
            }
        }

        if (totalRegistered == 0) {
            return 0;
        }

        return ((float) totalTurnout / totalRegistered) * 100;
    }

    public synchronized Map<String, Integer> getTotalVotesPerCandidate(int electionId) {
        Map<String, Integer> totals = new HashMap<>();

        for (PollingStationResult result : pollingStations.values()) {

            if (result.getElectionId() != electionId) {
                continue;
            }

            for (ControllerResult controllerResult : result.getControllerResults().values()) {

                for (Map.Entry<String, Integer> entry : controllerResult.getCandidateVotes().entrySet()) {
                    totals.put(
                            entry.getKey(),
                            totals.getOrDefault(entry.getKey(), 0) + entry.getValue()
                    );
                }

                break;
            }
        }

        return totals;
    }
    
    private void initializeElectionData() { // novo, inicijalizacija pocetnih podataka

        electionTypes.put(1, ElectionType.PARLIAMENTARY);

        electionTypes.put(2, ElectionType.PRESIDENTIAL);

        electionTypes.put(3, ElectionType.LOCAL);

        electionCandidates.put(
                1,
                List.of("ListaA", "ListaB", "ListaC")
        );

        electionCandidates.put(
                2,
                List.of("Kandidat1", "Kandidat2")
        );

        electionCandidates.put(
                3,
                List.of("LokalnaLista1", "LokalnaLista2")
        );

        Random random = new Random();

        for (int i = 1; i <= TOTAL_POLLING_STATIONS; i++) {

            int registeredVoters =
                    500 + random.nextInt(1501);

            registeredVotersByStation.put(
                    i,
                    registeredVoters
            );
        }
    }
}