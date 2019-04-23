package com.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.master.Master;
import java.util.List;
import java.util.ArrayList;

public class ClientFS {
	
	private Master m = new Master();
	
	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	private static Socket s;
	private String ip;
	private int port;

	public static final String root = "csci485";
	
	public enum FSReturnVals {
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
		
		
	}

	
	public ClientFS() {
		if (s != null) return;
		try {
			FileReader fr = new FileReader(new File(Master.configFilePath));
			
			BufferedReader bufferedReader = new BufferedReader(fr);
			String line;
			String lastEntry = "";
			while ((line = bufferedReader.readLine()) != null) {
				lastEntry = line;
			}
			fr.close();
			port = Integer.parseInt(lastEntry.substring(lastEntry.indexOf(':')+2));
			
			System.out.println("ClientFS got port: " + port);
			ip = "127.0.0.1";
	
			s = new Socket(ip, port);
			
			oos = new ObjectOutputStream(s.getOutputStream());
			oos.flush();
	
			ois = new ObjectInputStream(s.getInputStream());
	
		} catch(IOException ioe) {
			System.out.println("ioe in Client constructor: " + ioe.getMessage());
		}
	}
	
	
	
	
	
	
	
	/**
	 * Creates the specified dirname in the src directory 
	 * 
	 * Returns SrcDirNotExistent if the src directory does not exist 
	 * Returns DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		try {
			oos.writeInt(Master.CREATE_DIR);
			Master.sendString(oos, src);
			Master.sendString(oos, dirname);
			oos.flush();
			
			String result = new String(Master.readString(ois));
			return FSReturnVals.valueOf(result);
			
			//read in result from master
			
		} catch(IOException ioe) {
			return FSReturnVals.Fail;
		}

		//return m.masterCreateDir(src, dirname);
	}
	
	

	/**
	 * Deletes the specified dirname in the src directory 
	 * 
	 * Returns SrcDirNotExistent if the src directory does not exist 
	 * Returns DestDirExists if the specified dirname exists
	 * -- Returns DirNotEmpty when a non empty directory is deleted (attempted)
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		try {
			oos.writeInt(Master.DELETE_DIR);
			Master.sendString(oos, src);
			Master.sendString(oos, dirname);
			oos.flush();
			
			String result = new String(Master.readString(ois));
			return FSReturnVals.valueOf(result);
			
			//read in result from master
			
		} catch(IOException ioe) {
			return FSReturnVals.Fail;
		}

		//return m.masterCreateDir(src, dirname);
	}
	
	//unnecessary, thought we deleted a dir even if it had files in it
	public void emptyDir(File dir, String path, int numDir) {
		if(dir.list() == null) {
			if(numDir != 0) dir.delete();
			return;
		}
		for(String currFileStr : dir.list()) {
			File currFile = new File(path + "/" + currFileStr);
			boolean del = currFile.delete();
			if(!del) emptyDir(currFile, path+"/"+currFileStr, numDir+1);
			currFile.delete();
		}
		if(numDir != 0) dir.delete();
		
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * 
	 * Returns SrcDirNotExistent if the src directory does not exist 
	 * Returns DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String newName) {
		try {
			oos.writeInt(Master.RENAME_DIR);
			Master.sendString(oos, src);
			Master.sendString(oos, newName);
			oos.flush();
			
			String result = new String(Master.readString(ois));
			return FSReturnVals.valueOf(result);
			
			//read in result from master
			
		} catch(IOException ioe) {
			return FSReturnVals.Fail;
		}
	}

	/**
	 * Lists the content of the target directory
	 * 
	 * Returns SrcDirNotExistent if the target directory does not exist 
	 * Returns null if the target directory is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		try {
			oos.writeInt(Master.LIST_DIR);
			Master.sendString(oos, tgt);
			oos.flush();
			
			int numStrings = Master.getPayloadInt(ois);
			String[] results = new String[numStrings];
			for(int i=0; i<numStrings; i++) {
				results[i] = new String(Master.readString(ois));
			}
			return results;
			
			//read in result from master
			
		} catch(IOException ioe) {
			return null;
		}
	}

	/**
	 * Creates the specified filename in the target directory 
	 * 
	 * Returns SrcDirNotExistent if the target directory does not exist 
	 * Returns FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		try {
			oos.writeInt(Master.CREATE_FILE);
			Master.sendString(oos, tgtdir);
			Master.sendString(oos, filename);
			oos.flush();
			
			String result = new String(Master.readString(ois));
			return FSReturnVals.valueOf(result);
		} catch(IOException ioe) {
			return FSReturnVals.Fail;
		}
	}

	/**
	 * Deletes the specified filename from the tgtdir 
	 * 
	 * Returns SrcDirNotExistent if the target directory does not exist 
	 * Returns FileDoesNotExist if the specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		try {
			oos.writeInt(Master.DELETE_FILE);
			Master.sendString(oos, tgtdir);
			Master.sendString(oos, filename);
			oos.flush();
			
			String result = new String(Master.readString(ois));
			return FSReturnVals.valueOf(result);
		} catch(IOException ioe) {
			return FSReturnVals.Fail;
		}
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * 
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String filepath, FileHandle ofh) {
		//System.out.println("---OPENING FILE---");

		FSReturnVals result =  m.masterOpenFile(filepath, ofh);
		System.out.println("open file result: " + result);
		return result;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		ofh = null; //what else does this need to do???
		return FSReturnVals.Success;
//		return null;
	}
	
	

	
	
	


}

























