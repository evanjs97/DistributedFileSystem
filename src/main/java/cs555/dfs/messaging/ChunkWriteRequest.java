package cs555.dfs.messaging;

import cs555.dfs.util.ChunkUtil;

import java.io.*;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

public class ChunkWriteRequest implements Event{

	private final List<ChunkUtil> locations;
	private final String filename;
	private final byte[] chunkData;
	private final Instant lastModified;
	private final boolean replication;

	public LinkedList<ChunkUtil> getLocations() {
		return (LinkedList<ChunkUtil>) locations;
	}

	public String getFilename() {
		return filename;
	}

	public byte[] getChunkData() {
		return chunkData;
	}

	public Instant getLastModified() { return lastModified; }

	public boolean getReplication() { return this.replication; }

	@Override
	public Type getType() {
		return Type.CHUNK_WRITE_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeString(filename);
		messageMarshaller.writeChunkUtilList(locations);

		messageMarshaller.writeByteArr(chunkData);
		messageMarshaller.writeInstant(lastModified);
		messageMarshaller.writeBoolean(replication);

		return messageMarshaller.getMarshalledData();
	}

	public ChunkWriteRequest(LinkedList<ChunkUtil> locations, String filename,
							 byte[] chunkData, Instant lastModified, boolean replication) {
		this.locations = locations;
		this.filename = filename;
		this.chunkData = chunkData;
		this.lastModified = lastModified;
		this.replication = replication;

	}

	public ChunkWriteRequest(LinkedList<ChunkUtil> locations, String filename,
							 byte[] chunkData, Instant lastModified) {
		this.locations = locations;
		this.filename = filename;
		this.chunkData = chunkData;
		this.lastModified = lastModified;
		this.replication = true;

	}

	public ChunkWriteRequest(DataInputStream din) {
		Instant time = null;
		String name = "";
		this.locations = new LinkedList<>();
		byte[] chunk = null;
		boolean replication = true;
		try {
			MessageReader messageReader = new MessageReader(din);
			name = messageReader.readString();
			messageReader.readChunkUtilList(locations);
			chunk = messageReader.readByteArr();
			time = messageReader.readInstant();
			replication = messageReader.readBoolean();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		chunkData = chunk;
		filename = name;
		lastModified = time;
		this.replication = replication;
	}
}
