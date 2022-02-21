package JAva_Sourbah.Kafka_Flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;
public class flink_kafka_reciver_sender {
	Class.forName("com.mysql.jdbc.Driver");
	static String myUrl = "jdbc:mysql://localhost/fetch_blocks_DB";
	static Connection conn = DriverManager.getConnection(myUrl, "root", "");
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		String server = "localhost:9092";
		 String inputTopic = "testtopic";
		 String outputTopic = "outputtopic";  
		 StramStringOperation(server,inputTopic,outputTopic );

	}
	
	public static class StringCapitalizer implements MapFunction<String, String> {
	    @Override
	    public String map(String data) throws Exception {
	    	String data1= data.toUpperCase();
            String sql = "insert into  fetch_blocks values(data1)";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute(sql);
            System.out.println(data.toUpperCase());
            
	    }
	}
	
	public static void StramStringOperation(String server,String inputTopic, String outputTopic ) throws Exception {
	    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
	    FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
	    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, server);
	    DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
	  
	    stringInputStream.map(new StringCapitalizer()).addSink(flinkKafkaProducer);
	   
	    environment.execute();
	}
	
	public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(
			  String topic, String kafkaAddress) {
			 
			    Properties props = new Properties();
			    props.setProperty("bootstrap.servers", kafkaAddress);
			    //props.setProperty("group.id",kafkaGroup);
			    FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
			      topic, new SimpleStringSchema(), props);

			    return consumer;
	}
	
	public static FlinkKafkaProducer011<String> createStringProducer(
			  String topic, String kafkaAddress){

			    return new FlinkKafkaProducer011<>(kafkaAddress,
			      topic, new SimpleStringSchema());
	}


}
