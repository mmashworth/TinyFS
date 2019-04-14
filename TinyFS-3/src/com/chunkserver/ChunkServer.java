package com.chunkserver;

import java.io.File;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.io.FileNotFoundException;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;


import com.client.Client;
import com.interfaces.ChunkServerInterface;
import com.master.Master;
import com.client.RID;
import com.client.FileHandle;
import com.client.ClientFS.FSReturnVals;


/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String rootFilePath = "TinyFS-3/csci485/";	//or C:\\newfile.txt
	public final static String ClientConfigFile = "ClientConfig.txt";
	
	//Used for the file system
	public static long counter;
	
	public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer  
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
	
	public static final int AppendRecordCMD = 104;
	
	//Replies provided by the server
	public static final int TRUE = 1;
	public static final int FALSE = 0;
	
	//map from rootFilePath to its chunk handles
	//chunk handle = filename + slot number/index
	Map<String, List<String>> fileToChunks;
	
	public static final int FileHeaderLength = 4;
	public static final int CHUNK_SIZE = 4096;
	
	
	/*
	 * Sent from ClientRec,
	 * Append payload to the File with fh
	 * 
	 * Returns BadHandle if ofh is invalid 
	 * Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 */
	public FSReturnVals chunkServerAppendRecord(FileHandle fh, byte[] payload, RID recordID) {	
		if(!recordID.invalid()) return FSReturnVals.BadRecID;
		if(payload.length > CHUNK_SIZE - 8) return FSReturnVals.RecordTooLong;
				
		String totalFilepath = rootFilePath + removeFirstSlash(fh.getFileDir());
		String filepath = fh.getFileDir() + fh.getFileName();
		String filename = fh.getFileName();
		
		//if the file has no chunks, add it to the list
		if(!fileToChunks.containsKey(filepath)) {
			fileToChunks.put(filepath, new ArrayList<>());		
		}
		
		RandomAccessFile currFile = getFile(totalFilepath, filename);
		List<String> fileChunks = fileToChunks.get(filepath);
		int numChunks = fileChunks.size();

		if(numChunks == 0) { //this file has no chunks yet
			return appendRecordToEmptyChunk(currFile, payload, recordID, filepath, 1);
		}
		
		//if the last chunk has space append record to that, otherwise add it to a new chunk
		String lastChunk = fileChunks.get(fileChunks.size()-1);
		try {
			int chunkOffset = (numChunks-1)*CHUNK_SIZE;
			System.out.println("chunk offset: " + chunkOffset);
			currFile.seek(4 + chunkOffset); //get location of first free record
			
		
			int nextFreeRecordOffset = readIntAtOffset(currFile, 4+chunkOffset);
			System.out.println("First free record is at: " + nextFreeRecordOffset);
			
			//read value at firstFreeRecord+4
				//if this value is >= payload.length+8, insert the record here at offset
			int prevPointerOffset = 4+chunkOffset;
			while(nextFreeRecordOffset != -1) { //112
				System.out.println("next free record offset -->" + nextFreeRecordOffset);
				int freeRecordSpace = readIntAtOffset(currFile, nextFreeRecordOffset+4);
				System.out.println("Still have " + freeRecordSpace + " bytes of free space");
				
				
				if(freeRecordSpace >= payload.length + 8) {
					int nextOpenSpace = readIntAtOffset(currFile, nextFreeRecordOffset);

					writeIntAtOffset(currFile, 4+chunkOffset, nextFreeRecordOffset + payload.length);
					currFile.seek(nextFreeRecordOffset);
					currFile.write(payload);
					writeIntAtOffset(currFile, nextFreeRecordOffset+payload.length, nextOpenSpace);
					writeIntAtOffset(currFile, nextFreeRecordOffset+payload.length+4, freeRecordSpace-payload.length);
					System.out.println("Returning");
					
					recordID.setFilepath(filepath);
					recordID.setChunk(Integer.toString(numChunks+1));
					recordID.setOffset(nextFreeRecordOffset);
					recordID.setLength(payload.length);
					
					int numRecords = readIntAtOffset(currFile, chunkOffset) + 1;
					writeIntAtOffset(currFile, chunkOffset, numRecords);
					return FSReturnVals.Success;
				}
				
				prevPointerOffset = nextFreeRecordOffset;
				nextFreeRecordOffset = readIntAtOffset(currFile, nextFreeRecordOffset);

			}
		} catch(IOException ioe) {}
		
		return appendRecordToEmptyChunk(currFile, payload, recordID, filepath, fileChunks.size()+1);
	}
	
	
	
	
	
	
	
	public FSReturnVals appendRecordToEmptyChunk(RandomAccessFile currFile, byte[] payload, RID recordID, String filepath, int chunkNum) {
		System.out.println("------CREATING NEW CHUNK------");
		try {
			int chunkOffset = CHUNK_SIZE*(chunkNum-1);
			//allocate a space of size CHUNK_SIZE in the file
			currFile.setLength(CHUNK_SIZE*chunkNum);
			//get the starting location of that chunk
			currFile.seek(chunkOffset);
			//write 1 to the first byte (num records in the chunk)
			byte[] numRecords = ByteBuffer.allocate(4).putInt(1).array();
			currFile.write(numRecords);
			//write 4 + 4 + payload.length to the next four bytes (points to next free record)
			currFile.seek(4 + chunkOffset);
			byte[] nextFreeRecord = ByteBuffer.allocate(4).putInt(4+4+payload.length+chunkOffset).array();
			currFile.write(nextFreeRecord);
			//write payload to the next payload.length bytes
			currFile.seek(8 + chunkOffset);
			currFile.write(payload);
			//write null/-1 at 4 + 4 + payload.length (last free record, doesn't point to anything)
			currFile.seek(4+4+payload.length + chunkOffset);
			byte[] lastRecord = ByteBuffer.allocate(4).putInt(-1).array();
			currFile.write(lastRecord);
			//write CHUNK_SIZE-4-4-payload.length (free space remaining)
			currFile.seek(4+4+4+payload.length + chunkOffset);
			byte[] remainingSpace = ByteBuffer.allocate(4).putInt(CHUNK_SIZE-payload.length-4-4).array();
			currFile.write(remainingSpace);
		} catch(IOException ioe) { 
			System.out.println("append record ioe: " + ioe.getMessage()); 
			return FSReturnVals.Fail;
		}
		fileToChunks.get(filepath).add(Integer.toString(chunkNum)); //first chunk for this file
		recordID.setFilepath(filepath);
		recordID.setChunk(Integer.toString(chunkNum));
		recordID.setOffset(8);
		recordID.setLength(payload.length);
		return FSReturnVals.Success;
	}
	
	/* Returns a file with the filepath and filename
	 * 
	 * If the file exists, return the existing file
	 * Else, return a new file with a 0 as the first 4 bytes since
	 * it has no chunks yet
	 */
	public RandomAccessFile getFile(String filepath, String filename) {
		File dirPath = new File(filepath);
		dirPath.mkdirs();
		
		File f = new File(filepath+filename);
		try { f.createNewFile(); } 
		catch(IOException ioe) { System.out.println("getFile ioe: " + ioe.getMessage());}
		
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(filepath+filename, "rw");
			return file;	
		} catch (FileNotFoundException fnfe) {
			System.out.println("getFile fnfe: " + fnfe.getMessage());
		}
		return null;

	}
	
	
	
	
	
	
	
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		File dir = new File(rootFilePath);
		File[] fs = dir.listFiles();

		fileToChunks = new HashMap<String, List<String>>();
		
		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				try {
					cntrs[j] = Long.valueOf( fs[j].getName() ); 
				} catch(NumberFormatException nfe) {
					
				}
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String createChunk() {
		counter++;
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(rootFilePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(rootFilePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(rootFilePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	


	
	
	public static void ReadAndProcessRequests()
	{
		ChunkServer cs = new ChunkServer();
		
		//Used for communication with the Client via the network
		int ServerPort = 0; //Set to 0 to cause ServerSocket to allocate the port 
		ServerSocket commChanel = null;
		ObjectOutputStream WriteOutput = null;
		ObjectInputStream ReadInput = null;
		
		try {
			//Allocate a port and write it to the config file for the Client to consume
			commChanel = new ServerSocket(ServerPort);
			ServerPort=commChanel.getLocalPort();
			PrintWriter outWrite=new PrintWriter(new FileOutputStream(ClientConfigFile));
			outWrite.println("localhost:"+ServerPort);
			outWrite.close();
		} catch (IOException ex) {
			System.out.println("Error, failed to open a new socket to listen on.");
			ex.printStackTrace();
		}
		
		boolean done = false;
		Socket ClientConnection = null;  //A client's connection to the server

		while (!done){
			try {
				ClientConnection = commChanel.accept();
				ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
				WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
				
				//Use the existing input and output stream as long as the client is connected
				while (!ClientConnection.isClosed()) {
					int payloadsize =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
					if (payloadsize == -1) 
						break;
					int CMD = Client.ReadIntFromInputStream("ChunkServer", ReadInput);
					switch (CMD){
					case CreateChunkCMD:
						String chunkhandle = cs.createChunk();
						byte[] CHinbytes = chunkhandle.getBytes();
						WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
						WriteOutput.write(CHinbytes);
						WriteOutput.flush();
						break;

					case ReadChunkCMD:
						int offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						int payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
						byte[] CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						String ChunkHandle = (new String(CHinBytes)).toString();
						
						byte[] res = cs.readChunk(ChunkHandle, offset, payloadlength);
						
						if (res == null)
							WriteOutput.writeInt(ChunkServer.PayloadSZ);
						else {
							WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
							WriteOutput.write(res);
						}
						WriteOutput.flush();
						break;

					case WriteChunkCMD:
						offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						byte[] payload = Client.RecvPayload("ChunkServer", ReadInput, payloadlength);
						chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
						CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();

						//Call the writeChunk command
						if (cs.writeChunk(ChunkHandle, payload, offset))
							WriteOutput.writeInt(ChunkServer.TRUE);
						else WriteOutput.writeInt(ChunkServer.FALSE);
						
						WriteOutput.flush();
						break;

					default:
						System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
						break;
					}
				}
			} catch (IOException ex){
				System.out.println("Client Disconnected");
			} finally {
				try {
					if (ClientConnection != null)
						ClientConnection.close();
					if (ReadInput != null)
						ReadInput.close();
					if (WriteOutput != null) WriteOutput.close();
				} catch (IOException fex){
					System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
					fex.printStackTrace();
				}
			}
		}
	}

	public static void main(String args[])
	{
		ReadAndProcessRequests();
	}
	
	
	
	/*
	 * HELPERS
	 */
	
	public void writeIntAtOffset(RandomAccessFile file, int offset, int num) {
		try {
			file.seek(offset);
			byte[] numBytes = ByteBuffer.allocate(4).putInt(num).array();
			file.write(numBytes);
		}catch(IOException ioe) { 
			System.out.println("writeIntAtOffset ioe: " + ioe.getMessage());
			return;
		}
	}
	
	
	public int readIntAtOffset(RandomAccessFile file, int offset) {
		try {
			file.seek(offset); //get length of the free space
			byte[] offsetBytes = new byte[4];
			file.read(offsetBytes);
			return ByteBuffer.wrap(offsetBytes).getInt();
		} catch(IOException ioe) { 
			System.out.println("readIntAtOffset ioe: " + ioe.getMessage());
			return Integer.MIN_VALUE;
		}
	}
	
	public String removeLastSlash(String filepath) {
		if(filepath.charAt(filepath.length()-1) == '/')
			return filepath.substring(0, filepath.length()-1);
		return filepath;
	}
	
	public String removeFirstSlash(String filepath) {
		if(filepath.charAt(0) == '/')
			return filepath.substring(1);
		return filepath;
	}
	
	public String cutOffLastDir(String path) {
		path = path.substring(0, path.length()-1);
		while(path.charAt(path.length()-1) != '/') {
			path = path.substring(0, path.length()-1);	
		}
		return path;
	}
}
