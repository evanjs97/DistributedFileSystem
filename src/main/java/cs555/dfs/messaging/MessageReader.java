package cs555.dfs.messaging;

import cs555.dfs.util.ChunkMetadata;
import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.FileMetadata;

import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class MessageReader {

	private final DataInputStream din;

	public MessageReader(DataInputStream din) {
		this.din = din;
	}

	public int readInt() throws IOException {
		return din.readInt();
	}

	public boolean readBoolean() throws IOException {
		return din.readBoolean();
	}

	public long readLong() throws IOException {
		return din.readLong();
	}

	public double readDouble() throws IOException {
		return din.readDouble();
	}

	public Instant readInstant() throws IOException {
		return Instant.parse(readString());
	}

	public String readString() throws IOException{
		return new String(readByteArr());
	}

	public byte[] readByteArr() throws IOException{
		byte[] bytes = new byte[din.readInt()];
		din.readFully(bytes);
		return bytes;
	}

	public void readChunkMetadataList(List<ChunkMetadata> fileInfo) throws IOException {
		int listSize = din.readInt();
		for(int i = 0; i < listSize; i++) {
			fileInfo.add(new ChunkMetadata(this));
		}
	}

	public void readFileMetadataList(List<FileMetadata> fileInfo) throws IOException {
		int listSize = din.readInt();
		for(int i = 0; i < listSize; i++) {
			fileInfo.add(new FileMetadata(this));
		}
	}

	public void readChunkUtilList(List<ChunkUtil> chunkUtil) throws IOException {
		int listSize = din.readInt();
		for(int i = 0; i < listSize; i++) {
			chunkUtil.add(ChunkUtil.readChunkFromStream(this));
		}
	}

	public void close() throws IOException {
		din.close();
	}
}
