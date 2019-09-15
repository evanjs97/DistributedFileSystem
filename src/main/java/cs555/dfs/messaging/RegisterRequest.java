package cs555.dfs.messaging;

import java.io.*;

public class RegisterRequest implements Event{

	private final String hostname;
	private final int port;
	private final long freeSpace;

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}

	public long getFreeSpace() { return this.freeSpace; }

	@Override
	public Type getType() {
		return Type.REGISTER_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), hostname, port);
		messageMarshaller.writeLong(freeSpace);
		return messageMarshaller.getMarshalledData();
	}

	public RegisterRequest(String hostname, int port, long freeSpace) {
		this.hostname = hostname;
		this.port = port;
		this.freeSpace = freeSpace;
	}

	public RegisterRequest(DataInputStream din) {
		MessageReader messageReader = new MessageReader(din);

		String hostname = null;
		int port = 0;
		long freeSpace = -1;
		try {
			hostname = messageReader.readString();
			port = messageReader.readInt();
			freeSpace = messageReader.readLong();

			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.hostname = hostname;
		this.port = port;
		this.freeSpace = freeSpace;

	}
}
