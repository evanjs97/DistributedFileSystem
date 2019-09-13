package cs555.dfs.messaging;

import cs555.dfs.util.ChunkUtil;

import java.io.*;
import java.time.Instant;
import java.util.LinkedList;

public class ChunkWriteRequest implements Event{

	private final LinkedList<ChunkUtil> locations;
	private final String filename;
	private final byte[] chunkData;
	private final Instant lastModified;

	public LinkedList<ChunkUtil> getLocations() {
		return locations;
	}

	public String getFilename() {
		return filename;
	}

	public byte[] getChunkData() {
		return chunkData;
	}

	public Instant getLastModified() { return lastModified; }

	@Override
	public Type getType() {
		return Type.CHUNK_WRITE_REQUEST;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeString(filename);

		messageMarshaller.writeInt(locations.size());
		for(int i = 0; i < locations.size(); i++) {
			ChunkUtil chunkServer = locations.get(i);
			String chunk = (chunkServer.getHostname() + ":" + chunkServer.getPort());
			messageMarshaller.writeString(chunk);
		}

		messageMarshaller.writeByteArr(chunkData);
		messageMarshaller.writeInstant(lastModified);

		return messageMarshaller.getMarshalledData();
	}

	public ChunkWriteRequest(LinkedList<ChunkUtil> locations, String filename, byte[] chunkData, Instant lastModified) {
		this.locations = locations;
		this.filename = filename;
		this.chunkData = chunkData;
		this.lastModified = lastModified;

	}

	public ChunkWriteRequest(DataInputStream din) {
		Instant time = null;
		String name = "";
		this.locations = new LinkedList<>();
		byte[] chunk = null;
		try {
			MessageReader messageReader = new MessageReader(din);
			name = messageReader.readString();
			int numLocations = messageReader.readInt();

			for(int i = 0; i < numLocations; i++) {
				String[] split = messageReader.readString().split(":");
				locations.add(new ChunkUtil(split[0], Integer.parseInt(split[1])));
			}
			chunk = messageReader.readByteArr();
			time = messageReader.readInstant();

			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		chunkData = chunk;
		filename = name;
		lastModified = time;


//
//		String name = null;
//		try {
//			//read filename from stream
//			int nameLength = din.readInt();
//			byte[] nameBytes = new byte[nameLength];
//			din.readFully(nameBytes);
//			name = new String(nameBytes);
//
//			//read locations from stream
//			byte[] location;
//			int num = din.readInt();
//			for (int i = 0; i < num; i++) {
//				int length = din.readInt();
//
//				location = new byte[length];
//				din.readFully(location);
//
//				String[] split = new String(location).split(":");
//				locations.add(new ChunkUtil(split[0], Integer.parseInt(split[1])));
//			}

			//read chunk data from stream
//			int chunkSize = din.readInt();
//			chunk = new byte[chunkSize];
//			din.readFully(chunk);
//
//			din.close();

//		}catch(IOException ioe) {
//			ioe.printStackTrace();
//		}

	}
}
