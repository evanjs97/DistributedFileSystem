package cs555.dfs.util;

import cs555.dfs.messaging.MessageMarshaller;
import cs555.dfs.messaging.MessageReader;

import java.io.IOException;
import java.math.BigInteger;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunkUtil implements Comparable<ChunkUtil>{
	private int assignedChunks = 0;
	private final String hostname;
	private final int port;

	public ChunkUtil(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
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
		return hostname + ":" + port;
	}

	public void writeChunkToStream(MessageMarshaller messageMarshaller) throws IOException{
		messageMarshaller.writeString(hostname);
		messageMarshaller.writeInt(port);
	}

	public static ChunkUtil readChunkFromStream(MessageReader reader) throws IOException{
		String host = reader.readString();
		int port = reader.readInt();
		return new ChunkUtil(host, port);
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
		int thisChunk = this.assignedChunks;
		int otherChunk = other.assignedChunks;
		if(thisChunk != otherChunk) return Integer.compare(thisChunk, otherChunk);
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
			if(remainingBytes < length) length = remainingBytes;
			try {
				byte[] temp = Arrays.copyOfRange(chunk, start, start+length);
				start = start + length;
				checksums.add(SHAChecksum(temp));
			}catch (NoSuchAlgorithmException nsae) {
				nsae.printStackTrace();
				break;
			}catch (DigestException de) {
				de.printStackTrace();
				break;
			}


		}
		return checksums;
	}

	public static String SHAChecksum(byte[] bytes) throws NoSuchAlgorithmException, DigestException{
		MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
		return hashToHexString(messageDigest.digest(bytes));
	}

	public static String hashToHexString(byte[] hash) {
		BigInteger hashNumber = new BigInteger(1, hash);

		StringBuilder builder = new StringBuilder(hashNumber.toString(16));

		while(builder.length() < 40) {
			builder.insert(0, '0');
		}

		return builder.toString();
	}

	public static boolean getCorruptedFromChecksums(List<String> check1, List<String> check2) {
		for(int i = 0; i < check1.size(); i++) {
			if(!check1.get(i).equals(check2.get(i))) {
				System.out.println("Found corrupted chunk");
				return true;
			}
		}
		return false;
	}
}
