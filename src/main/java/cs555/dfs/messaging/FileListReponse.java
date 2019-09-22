package cs555.dfs.messaging;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileListReponse implements Event{

	private final List<String> files;

	public FileListReponse(List<String> files) {
		this.files = files;
	}

	public FileListReponse(DataInputStream din) {
		MessageReader messageReader = new MessageReader(din);
		files = new ArrayList<>();
		try {
			messageReader.readStringList(files);
			messageReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Type getType() {
		return Type.FILE_LIST_RESPONSE;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeStringList(files);
		return new byte[0];
	}
}
