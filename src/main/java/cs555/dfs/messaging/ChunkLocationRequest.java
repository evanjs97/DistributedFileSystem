package cs555.dfs.messaging;

import java.io.*;

public class ChunkLocationRequest implements Event{
	private int port;

	public int getPort() {
		return this.port;
	}

	@Override
	public Type getType() {
		return Type.CHUNK_LOCATION_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledData;
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());
		dout.writeInt(port);
		dout.flush();
		marshalledData = baOutStream.toByteArray();

		baOutStream.close();
		dout.close();
		return marshalledData;
	}

	public ChunkLocationRequest(int port) {
		this.port = port;
	}

	public ChunkLocationRequest(DataInputStream din) {
		try {
			port = din.readInt();

			din.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
