package cs555.dfs.util;

public class ChunkUtil implements Comparable<ChunkUtil>{
	private int assignedChunks = 0;
	private final String hostname;
	private final int port;

	public ChunkUtil(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	public int getAssignedChunks() {
		return assignedChunks;
	}

	public void incrementAssignedChunks() {
		this.assignedChunks++;
	}

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}

	@Override
	public String toString() {
		return "ChunkServer: " + hostname + " running on port " + port;
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof ChunkUtil) {
			ChunkUtil d = (ChunkUtil) o;
			return this.hostname.equals(d.hostname) && this.port == d.port;
		}else return false;
	}

	@Override
	public int compareTo(ChunkUtil other) {
		if(this.assignedChunks != other.assignedChunks) return Integer.compare(this.assignedChunks, other.assignedChunks);
		int host = this.hostname.compareTo(other.hostname);
		if(host != 0) return host;
		else return Integer.compare(this.port, other.port);
	}

	@Override
	public int hashCode() {
		return hostname.hashCode() + port;
	}
}
