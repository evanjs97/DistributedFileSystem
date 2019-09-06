package cs555.dfs.util;

import java.math.BigInteger;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class Chunk {
	private final String chunkLocation;
	private List<String> checksums;

	public Chunk(String filename, byte[] chunk) {
		this.chunkLocation = filename;
		this.checksums = this.getChecksums(chunk);
	}

	private List<String> getChecksums(byte[] chunk) {
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
