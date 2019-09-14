package cs555.dfs.messaging;

import java.io.*;

public class RegisterRequest implements Event{

	private final String hostname;
	private final int port;

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}

	@Override
	public Type getType() {
		return Type.REGISTER_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.marshallIntStringInt(getType().getValue(), hostname, port);
		return messageMarshaller.getMarshalledData();
	}

	public RegisterRequest(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	public RegisterRequest(DataInputStream din) {
		MessageReader messageReader = new MessageReader(din);

		String hostname = null;
		int port = 0;
		try {
			hostname = messageReader.readString();
			port = messageReader.readInt();

			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.hostname = hostname;
		this.port = port;

	}
}
