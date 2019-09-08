package cs555.dfs.messaging;

import java.io.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ChunkReadResponse implements Event{

	private final byte[] chunk;
	private final String filename;
	private final boolean success;
	private final List<Integer> corruptions;

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

	public boolean isSuccess() { return success; }

	public List<Integer> getCorruptions() { return corruptions; }

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledData;

		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());

		dout.writeBoolean(success);
		if(success) {
			dout.writeInt(chunk.length);
			dout.write(chunk);
		}else {
			dout.writeInt(corruptions.size());
			for(Integer i : corruptions) {
				dout.writeInt(i);
			}
		}

		byte[] nameBytes = filename.getBytes();
		dout.writeInt(nameBytes.length);
		dout.write(nameBytes);

		dout.flush();
		marshalledData = baOutStream.toByteArray();
		baOutStream.close();
		dout.close();

		return marshalledData;
	}

	public ChunkReadResponse(byte[] bytes, String filename, List<Integer> corruptions) {
		this.chunk = bytes;
		this.filename = filename;
		this.success = !corruptions.isEmpty();
		this.corruptions = Collections.unmodifiableList(corruptions);
	}

	public ChunkReadResponse(DataInputStream din) {
		byte[] chunk = null;
		String filename = "";
		boolean success = true;
		List<Integer> corruptions = new LinkedList<>();

		try{
			success = din.readBoolean();

			if(success) {
				int chunkSize = din.readInt();
				chunk = new byte[chunkSize];
				din.readFully(chunk);
			}else {
				int corruptionsLength = din.readInt();
				for(int i = 0; i < corruptionsLength; i++) {
					corruptions.add(din.readInt());
				}
			}

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
		this.success = success;
		this.corruptions = corruptions;
	}


}
