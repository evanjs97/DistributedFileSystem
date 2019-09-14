package cs555.dfs.messaging;

import java.io.*;

public class ChunkDestinationRequest implements Event{
	private int port;

	public int getPort() {
		return this.port;
	}

	@Override
	public Type getType() {
		return Type.CHUNK_DESTINATION_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeInt(port);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkDestinationRequest(int port) {
		this.port = port;
	}

	public ChunkDestinationRequest(DataInputStream din) {
		MessageReader messageReader = new MessageReader(din);
		try {
			port = messageReader.readInt();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
