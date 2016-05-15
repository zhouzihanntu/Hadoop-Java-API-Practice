package hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class java1 {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		mkDir(conf);
		putData(conf);
	}
	
	//Create Directory in hdfs
	private static void mkDir(Configuration conf) throws Exception {
		FileSystem fs=FileSystem.get(new URI("hdfs://192.168.102.132:9000/"), conf);
		Path dir = new Path("/user/FileSystemTest");
		boolean result = fs.mkdirs(dir);
		System.out.println("make dir :" + result);
		fs.close();
	}
	
	//Upload file/directory to hdfs
	private static void putData(Configuration conf) throws Exception{
		Path src = new Path("/Users/zhouzihan/Downloads/test.txt");
		FileSystem fs=FileSystem.get(new URI("hdfs://192.168.102.132:9000/"), conf);
		Path dire = new Path("/user/FileSystemTest");
		fs.copyFromLocalFile(src,dire);
		System.out.println("put data :" + conf.get("fs.default.name"));
		FileStatus files[] = fs.listStatus(dire);
		for(FileStatus file:files){
			System.out.println(file.getPath());
		}
	}
	
	//Download file/directory from hdfs
	private static void getData(Configuration conf) throws Exception{
		Path loc =new Path("/Users/zhouzihan/Desktop");
		FileSystem fs=FileSystem.get(new URI("hdfs://192.168.102.132:9000/"), conf);
		Path remote = new Path("/user/FileSystemTest/test.txt");
		fs.copyToLocalFile(false, remote, loc);
		System.out.println("copy data :" + conf.get("fs.default.name"));
	}
	
	//list files
	private static void list(Configuration conf) throws Exception{
		FileSystem fs=FileSystem.get(new URI("hdfs://192.168.102.132:9000/"), conf);
		Path listFile = new Path("/user/FileSystemTest");
		FileStatus files[] = fs.listStatus(listFile);
		for(int i=0; i<files.length;++i){
			System.out.println(files[i].getPath().toString());
		}
		fs.close();
	}

}
