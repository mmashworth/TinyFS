package com.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import com.master.Master;
import com.client.FileHandle;
import com.client.ClientFS.FSReturnVals;


/*the purpose is to direct clients which server to connect to.
the master waits for a connection, determines whether it is client or server
then creates the appropriate thread for communication*/
public class MasterClientThread extends Thread {

	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private Master m;
	private Socket clientSocket;
	
	public MasterClientThread(Socket s, Master ms, ObjectOutputStream oos, ObjectInputStream ois  ) {
		this.m = ms;
		this.clientSocket = s;
		this.oos = oos;
		this.ois = ois;
		
		this.start();
	}

	//public void sendMessage(String message) to client.
	/*public void sendMessage(MasterMessage m) {
//		pw.println(message);
//		pw.flush();
		try {
			oos.writeObject(mm);
			oos.flush();
		} catch (IOException ioe) {
			System.out.println("ioe: " + ioe.getMessage());
		}
	}*/
	
	public int getMessageCode() {
		try {
			return ois.readInt();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return -1;
		}
	}
	
	public void run() {
		int command;
		
		try { // stay in loop until connection disconnects. https://stackoverflow.com/questions/1390024/how-do-i-check-if-a-socket-is-currently-connected-in-java
			while((command = Master.getPayloadInt(ois)) != -1) {
	
				//read in command identifier and switch based on that
				System.out.println("Received command: " + command);
				
				String param1 = null, param2 = null, param3 = null;
				
				if(command == 4 || command == 7) {
					param1 = Master.readString(ois);
				}
				else if(command >= 1 && command <= 6) {
					param1 = Master.readString(ois);
					param2 = Master.readString(ois);
				}
				else if(command == 8) {
					param1 = Master.readString(ois);
					param2 = Master.readString(ois);
					param3 = Master.readString(ois);
				}
				
				if(command == Master.CREATE_DIR) {
					FSReturnVals result = m.masterCreateDir(param1, param2);
					Master.sendResultToClient(oos, result);
				}
				else if(command == Master.DELETE_DIR) {
					FSReturnVals result = m.masterDeleteDir(param1, param2);
					Master.sendResultToClient(oos, result);
				}
				else if(command == Master.RENAME_DIR) {
					FSReturnVals result = m.masterRenameDir(param1, param2);
					Master.sendResultToClient(oos, result);
				}
				else if(command == Master.LIST_DIR) {
					String[] results = m.masterListDir(param1);
					oos.writeInt(results.length);
					for(String s : results) {
						Master.sendString(oos, s);
					}
					oos.flush();
				}
				else if(command == Master.CREATE_FILE) {
					FSReturnVals result = m.masterCreateFile(param1, param2);
					Master.sendResultToClient(oos, result);
				}
				else if(command == Master.DELETE_FILE) {
					FSReturnVals result = m.masterDeleteFile(param1, param2);
					Master.sendResultToClient(oos, result);
				}
				else if(command == Master.OPEN_FILE) {
					FileHandle ofh = new FileHandle();
					FSReturnVals result = m.masterOpenFile(param1, ofh);
					System.out.println("result: " + result);
					Master.sendString(oos, ofh.getFileDir());
					Master.sendString(oos, ofh.getFileName());
					Master.sendResultToClient(oos, result);
				}
				else if(command == Master.CLOSE_FILE) {
					FileHandle fh = new FileHandle(param2, param3);
					m.masterOpenFile(param1, fh);
					m.masterCloseFile(param1, fh);
				}
				else {
					//System.out.println("Could not parse command");
				}
				
				if ((command > 0 && command < 9) && command != 4) {
					ArrayList<String> params = new ArrayList<String>();
					params.add(param1);
					if (param2 != null) {
						params.add(param2);
					}
					if (param3 != null) {
						params.add(param3);
					}
					m.logTransaction(command, params);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error in client thread communication: " + e.getMessage());
		}
		System.out.println("Client connection terminated ");
		m.cleanMaster();
	}


}