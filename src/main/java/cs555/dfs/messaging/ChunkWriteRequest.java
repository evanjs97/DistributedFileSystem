package cs555.dfs.messaging;

import cs555.dfs.util.ChunkUtil;

import java.io.*;
import java.util.LinkedList;

public class ChunkWriteRequest implements Event{

	private final LinkedList<ChunkUtil> locations;
	private final String filename;
	private final byte[] chunkData;

	public LinkedList<ChunkUtil> getLocations() {
		return locations;
	}

	public String getFilename() {
		return filename;
	}

	public byte[] getChunkData() {
		return chunkData;
	}

	@Override
	public Type getType() {
		return Type.CHUNK_WRITE_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledData;
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		//write type to stream
		dout.writeInt(getType().getValue());

		//write filename to stream
		byte[] nameBytes = filename.getBytes();
		dout.writeInt(nameBytes.length);
		dout.write(nameBytes);

		//write locations to stream
		byte[] chunkBytes;
		dout.writeInt(locations.size());
		for(int i = 0; i < locations.size(); i++) {
			ChunkUtil chunkServer = locations.get(i);
			chunkBytes = (chunkServer.getHostname() + ":" + chunkServer.getPort()).getBytes();
			dout.writeInt(chunkBytes.length);
			dout.write(chunkBytes);
		}

		//write chunk data to stream
		dout.writeInt(chunkData.length);
		dout.write(chunkData);


		dout.flush();
		marshalledData = baOutStream.toByteArray();

		baOutStream.close();
		dout.close();
		return marshalledData;
	}

	public ChunkWriteRequest(LinkedList<ChunkUtil> locations, String filename, byte[] chunkData) {
		this.locations = locations;
		this.filename = filename;
		this.chunkData = chunkData;

	}

	public ChunkWriteRequest(DataInputStream din) {
		this.locations = new LinkedList<>();
		byte[] chunk = null;
		String name = null;
		try {
			//read filename from stream
			int nameLength = din.readInt();
			byte[] nameBytes = new byte[nameLength];
			din.readFully(nameBytes);
			name = new String(nameBytes);

			//read locations from stream
			byte[] location;
			int num = din.readInt();
			for (int i = 0; i < num; i++) {
				int length = din.readInt();

				location = new byte[length];
				din.readFully(location);

				String[] split = new String(location).split(":");
				locations.add(new ChunkUtil(split[0], Integer.parseInt(split[1])));
			}

			//read chunk data from stream
			int chunkSize = din.readInt();
			chunk = new byte[chunkSize];
			din.readFully(chunk);

			din.close();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		chunkData = chunk;
		filename = name;
	}
}
