package cs555.dfs.util;

public class ChunkUtil implements Comparable<ChunkUtil>{
	private int storedChunks = 0;
	private final String hostname;
	private final int port;

	public ChunkUtil(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	public int getStoredChunks() {
		return storedChunks;
	}

	public void incrementStoredChunks() {
		this.storedChunks++;
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
		if(this.storedChunks != other.storedChunks) return Integer.compare(this.storedChunks, other.storedChunks);
		int host = this.hostname.compareTo(other.hostname);
		if(host != 0) return host;
		else return Integer.compare(this.port, other.port);
	}
}
