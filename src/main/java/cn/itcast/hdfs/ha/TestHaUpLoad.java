package cn.itcast.hdfs.ha;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * 
 * 如果访问的是HA集群，则一定需要把core-site.xml hdfs-site.xml配置文件放置到项目的classpath下
 * 以便让客户端能够理解hdfs://bi/ 中的bi是一个ha机制中的namenode----nameservice
 * 以及知道bi下面具体的namenode通信地址
 * 
 * @author AllenWoon
 *
 */
public class TestHaUpLoad {
		public static void main(String[] args) throws Exception{
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://bi/");
			FileSystem fileSystem = FileSystem.get(new URI("hdfs://bi/"), configuration,"root");
			
			fileSystem.copyFromLocalFile(new Path("D:/sparksql_textdata.csv"), new Path("/"));
			fileSystem.close();
		}
}
