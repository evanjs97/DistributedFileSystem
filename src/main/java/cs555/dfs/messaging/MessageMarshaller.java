package cs555.dfs.messaging;

import cs555.dfs.util.ChunkUtil;
import cs555.dfs.util.FileMetadata;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class MessageMarshaller {

	private final ByteArrayOutputStream baOutStream;
	private final DataOutputStream dout;

	public MessageMarshaller() {
		baOutStream = new ByteArrayOutputStream();
		dout = new DataOutputStream(new BufferedOutputStream(baOutStream));
	}

	public void writeInt(int value) throws IOException{
		dout.writeInt(value);
	}

	public void writeBoolean(boolean value) throws IOException {
		dout.writeBoolean(value);
	}

	public void writeInstant(Instant time) throws IOException {
		writeString(time.toString());
	}

	public void writeString(String str) throws IOException {
		byte[] strBytes = str.getBytes();
		dout.writeInt(strBytes.length);
		dout.write(strBytes);
	}

	public void writeByteArr(byte[] arr) throws IOException {
		dout.writeInt(arr.length);
		dout.write(arr);
	}

	public void writeMetadataList(List<FileMetadata> list) throws IOException {
		writeInt(list.size());
		for(FileMetadata metadata : list) {
			metadata.writeToStream(this);
		}
	}

	public void writeChunkUtilList(List<ChunkUtil> list) throws IOException {
		writeInt(list.size());
		for(ChunkUtil chunkUtil : list) {
			chunkUtil.writeChunkToStream(this);
		}
	}

	public void marshallIntStringInt(int value1, String str, int value2) throws IOException {
		writeInt(value1);
		writeString(str);
		writeInt(value2);
	}

	public byte[] getMarshalledData() throws IOException {
		dout.flush();
		byte[] marshalledData = baOutStream.toByteArray();
		baOutStream.close();
		dout.close();
		return marshalledData;
	}
}
