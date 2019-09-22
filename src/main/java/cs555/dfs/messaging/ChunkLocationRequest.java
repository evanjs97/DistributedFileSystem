package cs555.dfs.messaging;

import java.io.*;

public class ChunkLocationRequest implements Event{

	private final String filename;
	private final int port;
	private final int startChunk;
	private final int endChunk;
	private final int numShards;


	@Override
	public Type getType() {
		return Type.CHUNK_LOCATION_REQUEST;
	}

	public String getFilename() {
		return this.filename;
	}

	public int getPort() {
		return this.port;
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

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), filename, port);
		messageMarshaller.writeInt(startChunk);
		messageMarshaller.writeInt(endChunk);
		messageMarshaller.writeInt(numShards);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkLocationRequest(String filename, int port) {
		this(filename, 0,0,port);
	}

	public ChunkLocationRequest(String filename, int startChunk, int endChunk, int port) {
		this(filename, startChunk, endChunk, port, 0);
	}

	public ChunkLocationRequest(String filename, int startChunk, int endChunk, int port, int numShards) {
		this.filename = filename;
		this.startChunk = startChunk;
		this.endChunk = endChunk;
		this.port = port;
		this.numShards = numShards;
	}

	public ChunkLocationRequest(DataInputStream din) {
		String filename = "";
		int port = 0;
		int start = 0;
		int end = 0;
		int shards = 0;
		try {
			MessageReader messageReader = new MessageReader(din);
			filename = messageReader.readString();
			port = messageReader.readInt();
			start = messageReader.readInt();
			end = messageReader.readInt();
			shards = messageReader.readInt();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
		this.port = port;
		this.startChunk = start;
		this.endChunk = end;
		this.numShards = shards;
	}
}
