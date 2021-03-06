package com.master;

import java.util.Map;
import java.util.Scanner;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.TreeMap;

import com.client.ClientFS.FSReturnVals;
import com.chunkserver.ChunkServer;
import com.client.FileHandle;

/* FSReturnVals values
 
DirExists,         // Returned by CreateDir when directory exists
DirNotEmpty,       // Returned when a non-empty directory is deleted
SrcDirNotExistent, // Returned when source directory does not exist
DestDirExists,     // Returned when a destination directory exists
FileExists,        // Returned when a file exists
FileDoesNotExist,  // Returns when a file does not exist
BadHandle,         // Returned when the handle for an open file is not valid
RecordTooLong,     // Returned when a record size is larger than chunk size
BadRecID,          // The specified RID is not valid, used by DeleteRecord
RecDoesNotExist,   // The specified record does not exist, used by DeleteRecord
NotImplemented,    // Specific to CSCI 485 and its unit tests
Success,           // Returned when a method succeeds
Fail               // Returned when a method fails
*/

public class Master {
	
	public static final int CREATE_DIR = 1;
	public static final int DELETE_DIR = 2;
	public static final int RENAME_DIR = 3;
	public static final int LIST_DIR = 4;
	public static final int CREATE_FILE = 5;
	public static final int DELETE_FILE = 6;
	public static final int OPEN_FILE = 7;
	public static final int CLOSE_FILE = 8;
	
	public static final String root = "TinyFS-3/csci485";
	public static final String logPath = root + "/logs/";
	public static final String logMeta = "logmeta.txt";
	public static final String namespacePath = root + "/namespace.txt";
	public static final String dirFilePath = root + "/dirFile.txt";
	public static final String configFilePath = "TinyFS-3/MasterConfig.txt/";
	
	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	private static ServerSocket ss;
	private static Socket s;
	private static int port;
	private static String ip;
	public static int numLogs = 0;
	public static int numTransactions = 0;
	
	public static final int SERVER_CONNEC = 1; //connection is from a chunk server
	public static final int CLIENT_CONNEC = 2; //connection is from a chunk client

	private Vector<MasterClientThread> clientThreads;
	
	//maps from a filepath to the contents of that directory
	private static Map<String, List<String> > namespace;
	//mapping from a directory to its files
	private static Map<String, List<FileHandle> > dirToFiles;
	//TODO: need a mapping from file handles to chunk handles
	
	
	
	// private Map<String, String> fileToChunkMap;
	
	public void cleanMaster() {
		namespace = new LinkedHashMap<>();
		namespace.put("/", new ArrayList<String>());
		clientThreads = new Vector<>();
		dirToFiles = new HashMap<>();	
	}

	public Master() {
		namespace = new LinkedHashMap<>();
		namespace.put("/", new ArrayList<String>());
		dirToFiles = new HashMap<>();
		clientThreads = new Vector<>();
		port = 0;
	}
	
	public static void main(String[] args) {
		Master m = new Master();
		m.startMaster();
	}
	
	public void startMaster() {
		Master m = new Master();
		try {
			ss = new ServerSocket(port);
			port = ss.getLocalPort(); //get port the server bound to
			
			System.out.println("Master bound to port: " + port);
			
			//get ip address
			//try { 
				//ip = InetAddress.getLocalHost().toString();	            
	        //} catch (UnknownHostException e) { System.out.println("start master error: " + e.getMessage()); }
			
			//Write port to config file
			//System.out.println("Writing port to file");
			FileWriter fw = new FileWriter(configFilePath);
			PrintWriter pw = new PrintWriter(fw);
			pw.println("port : " + Integer.toString(port));
			pw.close();
			
			// Initialize log file
			File logMetaFile = new File(logPath + logMeta);
			if (logMetaFile.exists()) {
				Scanner scan = new Scanner(logMetaFile);
				if (scan.hasNext()) {
					numLogs = scan.nextInt();
				}
				scan.close();
			} else {
				FileWriter writer = new FileWriter(logMetaFile, false);
				writer.write(((Integer)numLogs).toString());
				writer.close();
			}
			
			// Initialize namespace
			File namespaceFile = new File(namespacePath);
			if (namespaceFile.exists()) {
				Scanner scan = new Scanner(namespaceFile);
				while(scan.hasNextLine()) {
					String line = scan.nextLine();
					List<String> keyValues = Arrays.asList(line.split("\\s+"));
					namespace.put(keyValues.get(0), new ArrayList<String>());
					namespace.put(keyValues.get(0), keyValues.subList(1, keyValues.size()));
					for (int i = 1; i < keyValues.size(); i++) {
						namespace.put(keyValues.get(0) + keyValues.get(i), new ArrayList<String>());
					}
				}
				scan.close();
			}
			
			// Initialize files
			File dirFile = new File(dirFilePath);
			if (dirFile.exists()) {
				Scanner scan = new Scanner(dirFile);
				while(scan.hasNextLine()) {
					String line = scan.nextLine();
					List<String> keyValues = Arrays.asList(line.split("\\s+"));
					ArrayList<FileHandle> fhs = new ArrayList<FileHandle>();
					for (int i = 0; i < keyValues.size() - 1; i++) {
						FileHandle fh = new FileHandle(keyValues.get(0), keyValues.subList(1, keyValues.size()).get(i));
						fhs.add(fh);
					}
					dirToFiles.put(keyValues.get(0), fhs);
				}
				scan.close();
			}
			
			// Redo log transactions
			File logFile = new File(logPath + Integer.toString(numLogs));
			if (logFile.exists()) {
				Scanner scan = new Scanner(logFile);
				String throwaway = scan.nextLine();
				while(scan.hasNextLine()) {
					String line = scan.nextLine();
					String[] pieces = line.split(" ");
					int command = Integer.parseInt(pieces[0]);
					if (command != -1) System.out.println("Redo command: " + command);
					
					String param1 = null, param2 = null, param3 = null;
					
					if(command == 4 || command == 7) {
						param1 = pieces[1];
					}
					else if(command >= 1 && command <= 6) {
						param1 = pieces[1];
						param2 = pieces[2];
					}
					else if(command == 8) {
						param1 = pieces[1];
						param2 = pieces[2];
						param3 = pieces[3];
					}
					
					if(command == CREATE_DIR) {
						m.masterCreateDir(param1, param2);
					}
					else if(command == DELETE_DIR) {
						m.masterDeleteDir(param1, param2);
					}
					else if(command == RENAME_DIR) {
						m.masterRenameDir(param1, param2);
					}
					else if(command == LIST_DIR) {
						m.masterListDir(param1);
					}
					else if(command == CREATE_FILE) {
						m.masterCreateFile(param1, param2);
					}
					else if(command == DELETE_FILE) {
						m.masterDeleteFile(param1, param2);
					}
					else if(command == OPEN_FILE) {
						FileHandle fh = new FileHandle();
						m.masterOpenFile(param1, fh);
					}
					else if(command == CLOSE_FILE) {
						FileHandle fh = new FileHandle(param2, param3);
						m.masterOpenFile(param1, fh);
						m.masterCloseFile(param1, fh);
					}
				}
			}
			
			
		} catch(IOException ioe) {
			System.out.println("ioe in startChunkServer binding to port: " + ioe.getMessage());
			return;
		}
		
		//TODO: need to read in logs here to restore master
		
		System.out.println("Now listening for connections");
		//continually listen on this port for new connections
		while(true) {
			try {
				//when new client/server connects, create new socket to communicate through
				s = ss.accept();
				System.out.println("Connection from: " + s.getInetAddress());
				
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				
				// get whether connection is from a client or a server
				int connec_type = getPayloadInt(ois);
				
				if (connec_type == CLIENT_CONNEC) {
					System.out.println("Received a new client connection.");
					MasterClientThread client = new MasterClientThread(s, m, oos, ois);
					m.clientThreads.add(client);
				} else if (connec_type == SERVER_CONNEC) {
					//TODO
				}
			} catch(IOException ioe) {
				System.out.println(ioe.getMessage());
			}
		} //end of while loop	
	} //end of start master
	
	
	/*
	 * CREATE DIRECTORIES
	 */
	public FSReturnVals masterCreateDir(String src, String dirName) {
		dirName = appendSlash(dirName);
		//return if src does not exist
		if(!namespace.containsKey(src)) return FSReturnVals.SrcDirNotExistent;
		
		//return if src already contains dirName
		List<String> srcContents = namespace.get(src);
		if(srcContents.contains(dirName)) return FSReturnVals.Success;
		
		//else add dirName to src and the new dir to the namespace
		System.out.println("Added " + src + dirName + " to the namespace");
		ArrayList<String> tempList = new ArrayList<String>(namespace.get(src));
		tempList.add(dirName);
		namespace.put(src, tempList);
		namespace.put(src + dirName, new ArrayList<String>());
		
		return FSReturnVals.Success;
	}
	
	/*
	 * DELETE DIRECTORIES
	 */
	public FSReturnVals masterDeleteDir(String src, String dirName) {
		dirName = appendSlash(dirName);
		//System.out.println("Deleting: " + src + dirName);

		if(!namespace.containsKey(src)) return FSReturnVals.SrcDirNotExistent;
				
		String targetPath = src + dirName;
		//dir exists and we're going to try to delete it
		if(namespace.containsKey(targetPath)) {
			//don't delete if it has contents in it
			if(namespace.get(targetPath).size() > 0) return FSReturnVals.DirNotEmpty;
			//otherwise remove from namespace
			
			//remove parent's reference to dir
			namespace.get(src).remove(dirName);
			//remove the actual dir
			namespace.remove(targetPath);
			//printNamespace();
			return FSReturnVals.DestDirExists;
		}

		return null; //not sure what to do if we attempt to delete a non-existent dir
	}
	
	/*
	 * RENAME DIRECTORES
	 */
	public FSReturnVals masterRenameDir(String src, String newName) {
		src = appendSlash(src);
		newName = appendSlash(newName);
		
		if(!namespace.containsKey(src)) return FSReturnVals.SrcDirNotExistent;
		if(namespace.containsKey(newName)) return FSReturnVals.DestDirExists;
		
		//rename actually directory
		List<String> srcDirs = namespace.remove(src);
		namespace.put(newName, srcDirs);

		//rename parent's reference to directory
		String parentDir = cutOffLastDir(src);
		namespace.get(parentDir).remove(getLastDir(src));
		namespace.get(parentDir).add(getLastDir(newName));
		
		return FSReturnVals.Success;
	}

	/*
	 * LIST DIRECTORIES
	 */
	public String[] masterListDir(String tgt) {
		tgt = appendSlash(tgt);
		if(!namespace.containsKey(tgt)) return null;
				
		List<String> filesAsList = new ArrayList<String>();
		masterListDirHelper(filesAsList, namespace.get(tgt), tgt);
		String[] files = filesAsList.toArray(new String[filesAsList.size()]);		
		
		return files;

	}

	public void masterListDirHelper(List<String> allFiles, List<String> currFiles, String path) {
		if(currFiles == null) { return; }
		for(String file : currFiles) {
			String filepath = path+file;
			filepath = filepath.substring(0, filepath.length()-1);
			allFiles.add(filepath);
			masterListDirHelper(allFiles, namespace.get(path+file), path+file);
		}
	}
	
	/*
	 * CREATE FILES
	 */
	public FSReturnVals masterCreateFile(String tgtdir, String filename) {
		tgtdir = appendSlash(tgtdir);
		//need a mapping from dirs to which files they contain
		
		//if no tgtdir, return SrcDirNotExistent
		if(!namespace.containsKey(tgtdir)) return FSReturnVals.SrcDirNotExistent;

		//if tgtdir has no files, add it to the mapping
		if(!dirToFiles.containsKey(tgtdir)) {
			ArrayList<FileHandle> dirFiles = new ArrayList<>();
			dirFiles.add(new FileHandle(tgtdir, filename));
			dirToFiles.put(tgtdir, dirFiles);
			return FSReturnVals.Success;
		}
		//if tgtdir has files, make sure tgtdir/filename isn't already in it
		FileHandle newFile = new FileHandle(tgtdir, filename);
		List<FileHandle> currFiles = dirToFiles.get(tgtdir);
		for(FileHandle fh : currFiles) {
			if(newFile.equals(fh)) return FSReturnVals.FileExists;
		}
		dirToFiles.get(tgtdir).add(newFile);
		return FSReturnVals.Success;		
	}
	
	/*
	 * DELETE FILES
	 */
	public FSReturnVals masterDeleteFile(String tgtdir, String filename) {
		//printFiles();
		
		//if tgtdir is not in namespace, return
		if(!namespace.containsKey(tgtdir)) return FSReturnVals.SrcDirNotExistent;

		FileHandle fileToDelete = new FileHandle(tgtdir, filename);
		for(FileHandle fh : dirToFiles.get(tgtdir)) {
			if(fileToDelete.equals(fh)) {
				dirToFiles.get(tgtdir).remove(fh);
				
				//if there are no more files in that List, remove the key
				if(dirToFiles.get(tgtdir).size() == 0) {
					//System.out.println("Removing dir " + tgtdir + " from map");
					dirToFiles.remove(tgtdir);
				}
				return FSReturnVals.Success;
			}
		}
		return FSReturnVals.FileDoesNotExist;
	}
	
	/*
	 * OPEN FILES
	 */
	//filepath of the form /aaa/bbb/.../zzz/ where zzz is the file to open
	public FSReturnVals masterOpenFile(String filepath, FileHandle ofh) {
		//printFiles();
		String tgtdir = cutOffLastDir(filepath);
		String filename = getLastDir(filepath);
		
		if(!namespace.containsKey(tgtdir)) return FSReturnVals.FileDoesNotExist;
		
		
		FileHandle tmp = new FileHandle(tgtdir, filename);
		for(FileHandle fh : dirToFiles.get(tgtdir)) {
			if(fh.equals(tmp)) {
				ofh.setFileDir(tgtdir);
				ofh.setFileName(filename);
				return FSReturnVals.Success;
			}
		}
		
		return FSReturnVals.FileDoesNotExist;
	}
	
	/*
	 * CLOSE FILES
	 */
	public FSReturnVals masterCloseFile(String filepath, FileHandle ofh) {
		//not really sure what this is supposed to do
		ofh = null;
		return FSReturnVals.Success;
	}
	
	/*
	 * HELPERS
	 */
	
	//for debugging
	public void printNamespace() {
		System.out.println("---BEGIN NAMESPACE---");
		TreeMap<String, List<String>> sorted = new TreeMap<>(); 
		  
        // Copy all data from hashMap into TreeMap 
        sorted.putAll(namespace); 
		for(String filepath : sorted.keySet()) {
			System.out.println(filepath);
			for(String s : namespace.get(filepath)) {
				System.out.println("\t" + s);
			}
		}
		System.out.println("---END NAMESPACE---");
	}
	
	public void printFiles() {
		System.out.println("---BEGIN FILES---");
		TreeMap<String, List<FileHandle>> sorted = new TreeMap<>(); 
		  
        // Copy all data from hashMap into TreeMap 
        sorted.putAll(dirToFiles); 
		for(String dir : sorted.keySet()) {
			System.out.println(dir);
			for(FileHandle fh : dirToFiles.get(dir)) {
				System.out.println("\t" + fh.getFileName());
			}
		}
		System.out.println("---END FILES---");
	}
	
	//assume path is of the form /.../.../.../
	public String cutOffLastDir(String path) {
		path = path.substring(0, path.length()-1);
		while(path.charAt(path.length()-1) != '/') {
			path = path.substring(0, path.length()-1);	
		}
		return path;
	}
	
	//assume path is of the form /.../.../.../
	public String getLastDir(String path) {
		String[] dirs = path.split("/");
		return dirs[dirs.length-1];
	}
	
	//appends a slash to the end of the string if it doesn't already have one
	//used because test cases are inconsistent in what they pass in
	public String appendSlash(String s) {
		if(s.charAt(s.length()-1) == '/') return s;
		return s + "/";
	}
	
	public void logTransaction(int command, ArrayList<String> params) {
		try {
			if (numTransactions >= 10) {
				numTransactions = 0;
				numLogs++;
				File logMetaFile = new File(logPath + "logMeta.txt");
				FileWriter fw = new FileWriter(logMetaFile, false);
				fw.write(((Integer)numLogs).toString());
				fw.close();
				
				File namespaceFile = new File(namespacePath);
				BufferedWriter bw = new BufferedWriter(new FileWriter(namespaceFile, false));
				
				for (Map.Entry<String, List<String>> entry : namespace.entrySet()) {
					String key = entry.getKey();
					List<String> values = entry.getValue();
					bw.write(key + " ");
					for (String value : values) {
						bw.write(value + " ");
					}
					bw.newLine();
				}
				bw.close();
				
				File dirFile = new File(dirFilePath);
				BufferedWriter writer = new BufferedWriter(new FileWriter(dirFile, false));
				
				for (Map.Entry<String, List<FileHandle>> entry : dirToFiles.entrySet()) {
					String key = entry.getKey();
					List<FileHandle> values = entry.getValue();
					writer.write(key + " ");
					for (FileHandle fh: values) {
						String fileName = fh.getFileName();
						writer.write(fileName + " ");
					}
					writer.newLine();
				}
				writer.close();
				
				/*for (String key : namespace.keySet()) {
					bw.write(key + " ");
					// System.out.print(key + " ");
					List<String> values = namespace.get(key);
					for (String value : values) {
						bw.write(value + " ");
						// System.out.print(value + " ");
					}
					bw.newLine();
				}
				bw.close();*/
			}
			
			File logFile = new File(logPath + Integer.toString(numLogs));
			BufferedWriter bw = new BufferedWriter(new FileWriter(logFile, true));
			String transaction = Integer.toString(command) + " ";
			for (String param: params) {
				transaction += param + " ";
			}
			bw.write(transaction);
			bw.newLine();
			bw.close();
			numTransactions++;
		} catch (IOException ioe) {
			System.out.println("ioe in logTransaction: " + ioe.getMessage());
		}
	}
	
	/*
	 * methods for getting ints/strings from streams
	 */
	
	public static void sendResultToClient(ObjectOutputStream oos, FSReturnVals result) {
		byte[] result_bytes = result.toString().getBytes();
		try {
			oos.writeInt(result_bytes.length);
			oos.write(result_bytes);
			oos.flush();
		} catch(IOException ioe) {
			System.out.println("sendStringToMaster ioe: " + ioe.getMessage());
		}
	}
	
	public static void sendString(ObjectOutputStream oos, String s) {
		byte[] s_bytes = s.getBytes();
		try {
			oos.writeInt(s_bytes.length);
			oos.write(s_bytes);
		} catch(IOException ioe) {
			System.out.println("sendStringToMaster ioe: " + ioe.getMessage());
		}
	}
	
	public static int getPayloadInt(ObjectInputStream ois) {
		byte[] payload = new byte[4];
		int result = -2;
		try {
			result = ois.read(payload, 0, 4);
		} catch(IOException ioe) {
			System.out.println("ioe in getPayloadInt on server: " + ioe.getMessage());
		}

		if(result == -1) return result;
		return (ByteBuffer.wrap(payload)).getInt();
	}
	
	
	public static String readString(ObjectInputStream ois) {
		try {
			int payloadSize = getPayloadInt(ois);
			
			byte[] payload = new byte[payloadSize];
			byte[] temp = new byte[payloadSize];
			int totalRead = 0;
			
			while(totalRead != payloadSize) {
				int currRead = -1;
				try {
					//read bytes from stream into byte array and add byte by byte to final
					//payload byte array
					currRead = ois.read(temp, 0, (payloadSize - totalRead));
					for(int i=0; i < currRead; i++) {
						payload[totalRead + i] = temp[i];
					}
				} catch(IOException ioe) {
					System.out.println("ChunkServer getPayload ioe: " + ioe.getMessage());
					try {
						s.close();
						System.out.println("closed client socket connection");
					} catch(IOException ioe2) {
						System.out.println("ioe in closing client socket connection: " + ioe2.getMessage());
					}
					return null;
				}
				if(currRead == -1) {
					System.out.println("error in reading payload");
					return null;
				} else {
					totalRead += currRead;
				}
			}
			
			String result = new String(payload);
			return result;
			
			
			
			
		} catch(Exception e) { System.out.println("master read string e: " + e.getMessage()); }
		return null;
	}
	
}
