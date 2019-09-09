package cs555.dfs.messaging;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class ChunkReadRequest implements Event{

	private final int port;
	private final String filename;
	private final List<Integer> chunkSlices;

	public int getPort() {
		return port;
	}

	public String getFilename() {
		return filename;
	}

	@Override
	public Type getType() {
		return Type.CHUNK_READ_REQUEST;
	}

	public List<Integer> getChunkSlices() {
		return this.chunkSlices;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledData;

		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());
		dout.writeInt(port);

		byte[] nameBytes = filename.getBytes();
		dout.writeInt(nameBytes.length);
		dout.write(nameBytes);

		dout.writeInt(chunkSlices.size());
		for(Integer slice : chunkSlices) {
			dout.writeInt(slice);
		}

		dout.flush();
		marshalledData = baOutStream.toByteArray();
		baOutStream.close();
		dout.close();

		return marshalledData;
	}

	public ChunkReadRequest(String filename, int port, List<Integer> chunkSlices) {
		this.filename = filename;
		this.port = port;
		this.chunkSlices = new LinkedList<>();
	}

	public ChunkReadRequest(DataInputStream din) {
		int port = 0;
		String filename = null;
		this.chunkSlices = new LinkedList<>();
		try {
			port = din.readInt();

			int nameLength = din.readInt();
			byte[] nameBytes = new byte[nameLength];
			din.readFully(nameBytes);
			filename = new String(nameBytes);

			int sliceLength = din.readInt();
			for(int i = 0; i < sliceLength; i++) {
				chunkSlices.add(din.readInt());
			}

			din.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.port = port;
		this.filename = filename;
	}

	public ChunkReadRequest(String filename, int port) {
		this.filename = filename;
		this.port = port;
		this.chunkSlices = new LinkedList<>();
	}
}
