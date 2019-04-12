package UnitTests3;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;

/**
 * UnitTest3 for Part 3 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */
public class UnitTest3 {
	
	public static int N = 755;
	static final String TestName = "Unit Test 3: ";

	public static void main(String[] args) {
		ClientFS cfs = new ClientFS();
		UnitTest2 ut2 = new UnitTest2();
		ut2.test2(cfs);

		//create dir root/shahramghan.../CSCI485
		System.out.println(TestName + "CreateDir /ShahramGhandeharizadeh/CSCI485");
		String dir1 = "ShahramGhandeharizadeh";
		FSReturnVals fsrv = cfs.CreateDir(ClientFS.root + "/" + dir1 + "/", "CSCI485");
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 3 result: fail!1");
    		return;
		}
		
		//creates files root/shahramghan.../lecturei for i=1...N
		System.out.println(TestName + "CreateFile Lecture1/2/.../N in /ShahramGhandeharizadeh/CSCI485");
		for(int i = 1; i <= N; i++){
			fsrv = cfs.CreateFile(ClientFS.root + "/" + dir1 + "/CSCI485/", "Lecture" + i);
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!2");
	    		return;
			}
		}
				
		//delete all lecture files
		System.out.println(TestName + "DeleteFile Lecture1/2/.../N in /ShahramGhandeharizadeh/CSCI485");
		for(int i = 1; i <= N; i++){
			fsrv = cfs.DeleteFile(ClientFS.root + "/" + dir1 + "/CSCI485/", "Lecture" + i);
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!3");
	    		return;
			}
		}
		
		//add all lecture files to root/shahram/2/
		System.out.println(TestName + "CreateFile /Shahram/2/Lecture1, /Shahram/2/Lecture2, ...., /Shahram/2/Lecture15");
		String dir2 = "Shahram";
		for(int i = 1; i <= N; i++){
			fsrv = cfs.CreateFile(ClientFS.root + "/" + dir2 + "/2i/", "Lecture" + i);
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!4");
	    		return;
			}
		}
		
		
		
		//delete all lecture files from root/shahram/2/
		System.out.println(TestName + "DeleteFile /Shahram/2/Lecture1, /Shahram/2/Lecture2, ...., /Shahram/2/Lecture15");
		for(int i = 1; i <= N; i++){
			fsrv = cfs.DeleteFile(ClientFS.root + "/" + dir2 + "/2i/", "Lecture" + i);
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!5");
	    		return;
			}
		}
		
		System.out.println(TestName + "Success!");
	}
}
