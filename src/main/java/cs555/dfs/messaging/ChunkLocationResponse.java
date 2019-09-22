package cs555.dfs.messaging;

import cs555.dfs.util.ChunkUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ChunkLocationResponse implements Event{

	private final boolean success;
	private final String filename;
	private final int startChunk;
	private final int endChunk;
	private final int numShards;
	private final List<ChunkUtil> locations;

	@Override
	public Type getType() {
		return Type.CHUNK_LOCATION_RESPONSE;
	}

	public List<ChunkUtil> getLocations() { return this.locations; }

	public boolean isSuccess() {
		return success;
	}

	public int getStartChunk() {
		return startChunk;
	}

	public int getEndChunk() {
		return endChunk;
	}

	public int getNumShards() {
		return numShards;
	}

	public String getFilename() {
		return this.filename;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeBoolean(success);
		messageMarshaller.writeString(filename);
		messageMarshaller.writeInt(startChunk);
		messageMarshaller.writeInt(endChunk);
		messageMarshaller.writeInt(numShards);
		messageMarshaller.writeChunkUtilList(locations, true);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkLocationResponse(boolean success, String filename, int startChunk, int endChunk, int numShards, List<ChunkUtil> locations) {

		this.success = success;
		this.filename = filename;
		this.startChunk = startChunk;
		this.endChunk = endChunk;
		this.numShards = numShards;
		this.locations = locations;
	}

	public ChunkLocationResponse(DataInputStream din) {
		boolean success = false;
		String filename = "";
		int start = 0;
		int end = 0;
		int shards = 0;
		this.locations = new ArrayList<>();
		try {
			MessageReader messageReader = new MessageReader(din);
			success = messageReader.readBoolean();
			filename = messageReader.readString();
			start = messageReader.readInt();
			end = messageReader.readInt();
			shards = messageReader.readInt();
			messageReader.readChunkUtilList(locations, true);
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.success = success;
		this.filename = filename;
		this.startChunk = start;
		this.endChunk = end;
		this.numShards = shards;
	}
}
