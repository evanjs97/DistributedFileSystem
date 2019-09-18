package cs555.dfs.util;

import cs555.dfs.messaging.MessageMarshaller;
import cs555.dfs.messaging.MessageReader;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileMetadata {
	private final String filename;
	private List<ChunkMetadata> chunks = new ArrayList<>();
	private Instant lastModified = null;
	private int version = 1;

	public String getFilename() {
		return filename;
	}

	public List<ChunkMetadata> getChunks() {
		return chunks;
	}

	public Instant getLastModified() {
		return lastModified;
	}

	public int getVersion() {
		return version;
	}

	public FileMetadata(String filename) {
		this.filename = filename;
	}

	public void addChunk(ChunkMetadata metadata) {
		chunks.add(metadata);
		if(lastModified == null || metadata.getLastModified().isAfter(lastModified)) {
			lastModified = metadata.getLastModified();
		}
		version = Math.max(version, metadata.getVersion());
	}

	public void writeToStream(MessageMarshaller messageMarshaller) {
		try {
			messageMarshaller.writeString(filename);
			messageMarshaller.writeInstant(lastModified);
			messageMarshaller.writeInt(version);
			messageMarshaller.writeChunkMetadataList(chunks);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String toString() {
		String time = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
				.withZone(ZoneId.systemDefault()).format(lastModified);
		return String.format("	--%s  chunks:%d version: %d   last modified: %s", filename, chunks.size(), version, time);
	}

	public FileMetadata(MessageReader reader) {
		String filename = "";
		Instant time = null;
		int version = 0;
		try {
			filename = reader.readString();
			time = reader.readInstant();
			version = reader.readInt();
			reader.readChunkMetadataList(chunks);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.filename =filename;
		this.lastModified = time;
		this.version = version;
	}
}
