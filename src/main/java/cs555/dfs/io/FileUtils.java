package cs555.dfs.io;

import cs555.dfs.transport.TCPSender;

import java.io.IOException;
import java.io.RandomAccessFile;

public class FileUtils {

	public void uploadFile(TCPSender sender, RandomAccessFile file) throws IOException{
		long fileSize = file.length();
		int bufferSize = 64 * 1024; //64KB
		int chunkSize = 64 * 1024; // 64KB
		long numSplits = fileSize / bufferSize;
		if(fileSize % bufferSize > 0) numSplits++;


		for(int i = 0; i < numSplits; i++) {
			//getSender
			//sendFile
		}

	}

	public void sendFile(TCPSender sender, RandomAccessFile file,int chunkSize, int bufferSize) throws IOException{
		for(int j = 0; j < chunkSize / bufferSize; j++) {
			byte[] buffer = new byte[bufferSize];
			int read = file.read(buffer);
			sender.sendData(buffer);
		}
		sender.flush();
	}

}
