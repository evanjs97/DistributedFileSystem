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
		byte[] marshalledData;
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());

		byte[] hostBytes = hostname.getBytes();
		dout.writeInt(hostBytes.length);
		dout.write(hostBytes);

		dout.writeInt(port);

		dout.flush();
		marshalledData = baOutStream.toByteArray();

		baOutStream.close();
		dout.close();
		return marshalledData;
	}

	public RegisterRequest(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	public RegisterRequest(DataInputStream din) {
		String hostname = null;
		int port = 0;
		try {
			int hostLength = din.readInt();
			byte[] hostBytes = new byte[hostLength];
			din.readFully(hostBytes);
			hostname = new String(hostBytes);

			port = din.readInt();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.hostname = hostname;
		this.port = port;

	}
}
