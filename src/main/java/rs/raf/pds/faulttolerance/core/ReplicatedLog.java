package rs.raf.pds.faulttolerance.core;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class ReplicatedLog {

	public static interface LogReplicator {
		public void replicateOnFollowers(Long entryAtIndex, byte[] data);
	}
	
	Long lastLogEntryIndex = 0L;
	final LogReplicator leaderReplicaNode;
	FileOutputStream fs;
	OutputStreamWriter writer;
	private final String fileName; //novo
	
	public ReplicatedLog(String fileName, LogReplicator node) throws FileNotFoundException {
		
		this.fileName = fileName; // novo 
		this.leaderReplicaNode = node;
		fs = new FileOutputStream(fileName, true); //nova izmena, da cuva log, dodala sam true
		writer = new OutputStreamWriter(fs);
		
		try { //novo, za log FT
		    lastLogEntryIndex = (long) readAllLogEntries().size();
		} catch (IOException e) {
		    lastLogEntryIndex = 0L;
		}
		
	}
		
	public synchronized void appendAndReplicate(byte[] commandBytes) throws IOException {
		Long lastLogEntryIndex = appendToLocalLog(commandBytes);
		leaderReplicaNode.replicateOnFollowers(lastLogEntryIndex, commandBytes);  
	}
	
	public Long appendToLocalLog(byte[] data) throws IOException {
		String s = new String(data);
		//System.out.println("Log #"+lastLogEntryIndex+":"+s);
		
		//fs.write(data);
		//fs.flush();
		writer.write(s);writer.write("\r\n");
		writer.flush();
		fs.flush();
		
		return ++lastLogEntryIndex;
	}

	public Long getLastLogEntryIndex() {
		return lastLogEntryIndex;
	}
	
	public List<String> readAllLogEntries() throws IOException { //dodato za citanje loga i vracanje stanja FT
	    Path path = Path.of(fileName);

	    if (!Files.exists(path)) {
	        return List.of();
	    }

	    return Files.readAllLines(path);
	}
	
	public String getFileName() { //dodala sam getter
	    return fileName;
	}
	
	public List<String> getLogEntriesFrom(long startIndex) throws IOException { // za sync
	    List<String> entries = readAllLogEntries();

	    if (startIndex <= 0) {
	        startIndex = 1;
	    }

	    int startArrayIndex = (int) startIndex - 1;

	    if (startArrayIndex >= entries.size()) {
	        return List.of();
	    }

	    return entries.subList(startArrayIndex, entries.size());
	}
	
}
