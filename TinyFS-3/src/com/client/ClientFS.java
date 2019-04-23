package com.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.interfaces.ClientInterface;
import com.master.Master;
import java.util.List;
import java.util.ArrayList;

public class ClientFS {
		
	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	private static Socket s;

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
	
	private static String masterIP = "127.0.0.1";
	private static int masterPort;
	
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
			masterPort = Integer.parseInt(lastEntry.substring(lastEntry.indexOf(':')+2));
			connectToMaster();
	
		} catch(IOException ioe) {
			System.out.println("ioe in Client constructor: " + ioe.getMessage());
		}
	}
	
	public void connectToMaster() {
		try {
			s = new Socket(masterIP, masterPort);
			
			oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeInt(ClientInterface.CLIENT_CONNEC); //let the master know this is a client connection
			oos.flush();
	
			ois = new ObjectInputStream(s.getInputStream());
		
		} catch(IOException ioe) {
			System.out.println("connectToMaster ioe: " + ioe.getMessage());
		}
	}
	public void disonnectFromMaster() {
		try {
			s.close();
			oos.close();
			ois.close();
		} catch(IOException ioe) {
			System.out.println("disconnectFromMaster ioe: " + ioe.getMessage());	
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
		
		// return m.masterDeleteDir(src, dirname);	
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
		
		// return m.masterRenameDir(src, newName);
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
		
		// return m.masterListDir(tgt);
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
//		System.out.println("---CREATING FILE---");
//		System.out.println("\ttgtdir: " + tgtdir);
//		System.out.println("\tfilename: " + filename);
		
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
		
		//return m.masterCreateFile(tgtdir, filename);
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
//		System.out.println("---DELETING FILE----");
//		System.out.println("\ttgtdir: " + tgtdir);
//		System.out.println("\tfilename: " + filename);
		
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
		
		/*FSReturnVals result =  m.masterDeleteFile(tgtdir, filename);
		System.out.println("deletion result: " + result);
		return result;*/
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
		
		try {
			oos.writeInt(Master.OPEN_FILE);
			Master.sendString(oos, filepath);
			//Master.sendString(oos, ofh.getFileDir());
			//Master.sendString(oos, ofh.getFileName());
			oos.flush();
			System.out.println("waiting for response from master now");
			
			String fileDir = Master.readString(ois);
			System.out.println(fileDir);
			String fileName = Master.readString(ois);
			System.out.println(fileName);
			ofh.setFileDir(fileDir);
			ofh.setFileName(fileName);
			String result = new String(Master.readString(ois));
			System.out.println("result: " + result);
			return FSReturnVals.valueOf(result);
		} catch(IOException ioe) {
			return FSReturnVals.Fail;
		}

		/*FSReturnVals result =  m.masterOpenFile(filepath, ofh);
		System.out.println("open file result: " + result);
		return result;*/
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

























