package com.master;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.io.File;
import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.TreeMap;

import com.client.ClientFS.FSReturnVals;
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
	
	//maps from a filepath to the contents of that directory
	private Map<String, List<String> > namespace;
	//mapping from a directory to its files
	private Map<String, List<FileHandle> > dirToFiles;
	//TODO: need a mapping from file handles to chunk handles
	
	
	
	private Map<String, String> fileToChunkMap;

	public Master() {
		namespace = new HashMap<>();
		namespace.put("/", new ArrayList<String>());
		
		dirToFiles = new HashMap<>();
	}
	
	
	
	/*
	 * CREATE DIRECTORIES
	 */
	public FSReturnVals masterCreateDir(String src, String dirName) {
		dirName = appendSlash(dirName);
		//return if src does not exist
		if(!namespace.containsKey(src)) return FSReturnVals.SrcDirNotExistent;
		
		//return if src already contains dirName
		List<String> srcContents = namespace.get(src);
		if(srcContents.contains(dirName)) return FSReturnVals.DestDirExists;
		
		//else add dirName to src and the new dir to the namespace
		System.out.println("Added " + src + dirName + " to the namespace");
		namespace.get(src).add(dirName);
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
		
		//System.out.println("\tcontains the src");
		
		String targetPath = src + dirName;
		//dir exists and we're going to try to delete it
		if(namespace.containsKey(targetPath)) {
			//System.out.println("\tcontains the dirName");
			//don't delete if it has contents in it
			if(namespace.get(targetPath).size() > 0) return FSReturnVals.DirNotEmpty;
			//otherwise remove from namespace
			
			//System.out.println("\tremoving dir");
			//remove parent's reference to dir
			namespace.get(src).remove(dirName);
			//actually remove dir
			namespace.remove(targetPath);
			printNamespace();
			return FSReturnVals.DestDirExists;
		}

		return null; //not sure what to do if we attempt to delete a non-existent dir...
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
		printFiles();
		
		//if tgtdir is not in namespace, return
		if(!namespace.containsKey(tgtdir)) return FSReturnVals.SrcDirNotExistent;

		FileHandle fileToDelete = new FileHandle(tgtdir, filename);
		for(FileHandle fh : dirToFiles.get(tgtdir)) {
			if(fileToDelete.equals(fh)) {
				dirToFiles.get(tgtdir).remove(fh);
				
				//if there are no more files in that List, remove the key
				if(dirToFiles.get(tgtdir).size() == 0) {
					System.out.println("Removing dir " + tgtdir + " from map");
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
		System.out.println("\ttgtdir: " + tgtdir);
		System.out.println("\tfilename: " + filename);
		
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
	
	
	
	
	
	
}
