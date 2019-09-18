package cs555.dfs.util;

import cs555.dfs.messaging.MessageMarshaller;
import cs555.dfs.messaging.MessageReader;

import java.io.IOException;
import java.math.BigInteger;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunkUtil implements Comparable<ChunkUtil> {
	private final AtomicInteger assignedChunks = new AtomicInteger(0);
	private final String hostname;
	private final int port;
	private double freeSpace;

	public ChunkUtil(String hostname, int port, double freeSpace) {
		this.hostname = hostname;
		this.port = port;
		this.freeSpace = freeSpace;
	}

	public synchronized final int incrementAssignedChunks() {
		return this.assignedChunks.incrementAndGet();
	}

	public synchronized final void setFreeSpace(double freeSpace) {
		this.freeSpace = freeSpace;
	}

	public synchronized final double getFreeSpace() {
		return this.freeSpace;
	}

	public final String getHostname() {
		return hostname;
	}

	public final int getPort() {
		return port;
	}

	@Override
	public final String toString() {
		return hostname + ":" + port;
	}

	public void writeChunkToStream(MessageMarshaller messageMarshaller) throws IOException {
		messageMarshaller.writeString(hostname);
		messageMarshaller.writeInt(port);
		messageMarshaller.writeDouble(freeSpace);
	}

	public static ChunkUtil readChunkFromStream(MessageReader reader) throws IOException {
		String host = reader.readString();
		int port = reader.readInt();
		double freeSpace = reader.readDouble();
		return new ChunkUtil(host, port, freeSpace);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ChunkUtil) {

			ChunkUtil d = (ChunkUtil) o;

			return this.hostname.equals(d.hostname) && this.port == d.port;
		} else return false;
	}

	@Override
	public int hashCode() {
		return this.hostname.hashCode() + port;
	}

	@Override
	public int compareTo(ChunkUtil other) {
		if(this.hostname.equals(other.hostname) && this.port == other.port) return 0;

		int myChunk = this.assignedChunks.get();
		int theirChunk = other.assignedChunks.get();
		double mySpace = this.getFreeSpace();
		double theirSpace = other.getFreeSpace();
		double chunkDiff = (myChunk == 0 || theirChunk == 0) ? 1.0 : ((double) theirChunk) / myChunk;
		double spaceDiff = mySpace / theirSpace;

		if (spaceDiff < 1.0 && chunkDiff < 1.0) { //if they have more space and less chunks, they go first
			return 1;
		} else if (spaceDiff > 1.0 && chunkDiff > 1.0) { //if I have more space and less chunks, I go first
			return -1;
		} else if (chunkDiff > (3.0 / 4.0) && chunkDiff < (4.0 / 3.0)) { //if chunk numbers are similar
			if (spaceDiff > 2.0) { //I have a lot more space
				return -1;
			} else if (spaceDiff < 0.5) { //they have a lot more space
				return 1;
			}
		} else if (spaceDiff > 0.5 && spaceDiff < 2.0) { //if space free is similar
			if (chunkDiff < 3.0 / 4.0) { //they have a lot fewer chunks
				return 1;
			} else if (chunkDiff > (4.0 / 3.0)) { //I have a lot fewer chunks
				return -1;
			}
		}

		int rand = ThreadLocalRandom.current().nextInt(1, 3); //if all aspects are similar return random order
		if (rand == 1) return -1;
		else return 1;
	}

//	public static List<String> getChecksums(byte[] chunk) {
//		List<String> checksums = new ArrayList<>();
//		int numChecksums = chunk.length / (8 * 1024);
//		if(chunk.length % (8 * 1024) > 0) {
//			numChecksums++;
//		}
//		int remainingBytes = chunk.length;
//		int start = 0;
//		for(int i = 0; i < numChecksums; i++) {
//			int length = 8 * 1024;
//			if(remainingBytes < length) length = remainingBytes;
//			try {
//				byte[] temp = Arrays.copyOfRange(chunk, start, start+length);
//				start = start + length;
//				checksums.add(SHAChecksum(temp));
//			}catch (NoSuchAlgorithmException nsae) {
//				nsae.printStackTrace();
//				break;
//			}catch (DigestException de) {
//				de.printStackTrace();
//				break;
//			}
//
//
//		}
//		return checksums;
//	}

	public static String SHAChecksum(byte[] bytes) throws NoSuchAlgorithmException, DigestException {
		MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
		return hashToHexString(messageDigest.digest(bytes));
	}

	public static String hashToHexString(byte[] hash) {
		BigInteger hashNumber = new BigInteger(1, hash);

		StringBuilder builder = new StringBuilder(hashNumber.toString(16));

		while (builder.length() < 40) {
			builder.insert(0, '0');
		}

		return builder.toString();
	}

}
