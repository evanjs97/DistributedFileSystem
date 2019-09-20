package cs555.dfs.messaging;

import java.io.*;

public class ChunkDestinationRequest implements Event{
	private final int port;
	private final String filename;
	private final int numLocations;

	public int getPort() {
		return this.port;
	}

	@Override
	public Type getType() {
		return Type.CHUNK_DESTINATION_REQUEST;
	}

	public String getFilename() {
		return this.filename;
	}

	public int getNumLocations() { return this.numLocations; }

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(),filename,port);
		messageMarshaller.writeInt(numLocations);
//		messageMarshaller.writeInt(getType().getValue());
//		messageMarshaller.writeInt(port);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkDestinationRequest(int port, String filename, int numLocations) {
		this.port = port;
		this.filename = filename;
		this.numLocations = numLocations;
	}

	public ChunkDestinationRequest(DataInputStream din) {
		MessageReader messageReader = new MessageReader(din);
		String filename = "";
		int port = 0;
		int numLocations = 0;
		try {
			filename = messageReader.readString();
			port = messageReader.readInt();
			numLocations = messageReader.readInt();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
		this.port = port;
		this.numLocations = numLocations;
	}
}
