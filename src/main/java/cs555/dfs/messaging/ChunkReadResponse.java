package cs555.dfs.messaging;

import java.io.*;

public class ChunkReadResponse implements Event{

	private final byte[] chunk;
	private final String filename;

	@Override
	public Type getType() {
		return Type.CHUNK_READ_RESPONSE;
	}

	public byte[] getChunk() {
		return chunk;
	}

	public String getFilename() {
		return filename;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledData;

		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());
		dout.writeInt(chunk.length);
		dout.write(chunk);

		byte[] nameBytes = filename.getBytes();
		dout.writeInt(nameBytes.length);
		dout.write(nameBytes);

		dout.flush();
		marshalledData = baOutStream.toByteArray();
		baOutStream.close();
		dout.close();

		return marshalledData;
	}

	public ChunkReadResponse(byte[] bytes, String filename) {
		this.chunk = bytes;
		this.filename = filename;
	}

	public ChunkReadResponse(DataInputStream din) {
		byte[] chunk = null;
		String filename = "";

		try{
			int chunkSize = din.readInt();
			chunk = new byte[chunkSize];
			din.readFully(chunk);

			int nameLength = din.readInt();
			byte[] nameBytes = new byte[nameLength];
			din.readFully(nameBytes);
			filename = new String(nameBytes);

			din.close();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.chunk = chunk;
		this.filename = filename;

	}


}
