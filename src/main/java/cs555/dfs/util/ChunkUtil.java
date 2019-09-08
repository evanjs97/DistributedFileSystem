package cs555.dfs.util;

import java.math.BigInteger;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

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

	public static List<String> getChecksums(byte[] chunk) {
		List<String> checksums = new ArrayList<>();
		int numChecksums = chunk.length / (8 * 1024);
		if(chunk.length % (8 * 1024) > 0) {
			numChecksums++;
		}
		int remainingBytes = chunk.length;
		int start = 0;
		for(int i = 0; i < numChecksums; i++) {
			int length = 8 * 1024;
			if(remainingBytes < 8 * 1024) length = remainingBytes;
			try {
				checksums.add(SHAChecksum(chunk, start, length));
			}catch (NoSuchAlgorithmException nsae) {
				nsae.printStackTrace();
				break;
			}catch (DigestException de) {
				de.printStackTrace();
				break;
			}
			start = start + length;

		}
		return checksums;
	}

	public static String SHAChecksum(byte[] bytes, int start, int end) throws NoSuchAlgorithmException, DigestException{
		System.out.println("HASHING");
		MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
		messageDigest.digest(bytes, start, end);
		return hashToHexString(bytes);
	}

	public static String hashToHexString(byte[] hash) {
		BigInteger hashNumber = new BigInteger(1, hash);

		StringBuilder builder = new StringBuilder(hashNumber.toString(16));

		while(builder.length() < 32) {
			builder.insert(0, '0');
		}

		return builder.toString();
	}
}
