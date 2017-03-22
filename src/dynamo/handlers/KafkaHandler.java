package dynamo.handlers;

import dynamo.AmbariRestClient;
import dynamo.serialization.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;

/******************************************************************************
* Sends data to a Kafka cluster.
******************************************************************************/
public class KafkaHandler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(KafkaHandler.class);
  private final Properties _kafkaProperties = new Properties();

  private ISerializer _serializer;
  private String _keySerializer;
  private String _valueSerializer;
  private String _topic;
  private String _ambariUrl;
  private String _kafkaCluster;
  private String _kafkaUsername;
  private String _kafkaPassword;
  private String _zookeeperHost = null;
  private String _brokerList = null;
  private ArrayList<String> _zookeeperNodes;
  private ArrayList<String> _kafkaBrokerNodes;
  private KafkaProducer<String, byte[]> _producer;

  /****************************************************************************
  * Creates an instance of KafkaHandler.
  ****************************************************************************/
  public KafkaHandler()
  {
    CheckConfiguration();
    DiscoverZookeeperAndBrokers();
    InitializeKafka();
  }

  /****************************************************************************
  * Creates an instance of KafkaHandler.
  *
  * @param topic
  *   The topic.
  ****************************************************************************/
  public KafkaHandler(String topic)
  {
    this (topic, null);
  }

  /****************************************************************************
  * Creates an instance of KafkaHandler.
  *
  * @param serializer
  *   The serializer.
  ****************************************************************************/
  public KafkaHandler(ISerializer serializer)
  {
    this(null, serializer);
  }

  /****************************************************************************
  * Creates an instance of KafkaHandler.
  *
  * @param topic
  *   The topic.
  * @param serializer
  *   The serializer.
  ****************************************************************************/
  public KafkaHandler(String topic, ISerializer serializer)
  {
    _topic = topic;
    _serializer = serializer;

    CheckConfiguration();
    DiscoverZookeeperAndBrokers();
    InitializeKafka();
  }

  /****************************************************************************
  * Serializes the data to a byte array and sends it a Kafka cluster.
  *
  * @param data
  *   The data.
  * @throws Throwable
  *   Any uncaught exception.
  ****************************************************************************/
  public void HandleData(Object data) throws Throwable
  {
    if (_serializer == null)
      throw new NullPointerException("No serialization set.");

    final byte[] serializedData = _serializer.Serialize(data);
    final ProducerRecord<String, byte[]> record = new ProducerRecord<>(_topic, serializedData);

    _producer = new KafkaProducer<>(_kafkaProperties);
    _producer.send(record);
    _producer.close();
  }

  /****************************************************************************
  * Checks for a properties file to load settings.
  ****************************************************************************/
  private void CheckConfiguration()
  {
    InputStream propertiesStream = null;

    try
    {
      final String propertiesFile = Paths.get("dynamo.properties").toAbsolutePath().toString();
      propertiesStream = new FileInputStream(propertiesFile);

      final Properties settings = new Properties();
      settings.load(propertiesStream);

      _ambariUrl = settings.getProperty("Handlers.Kafka.BaseUrl");
      _kafkaCluster = settings.getProperty("Handlers.Kafka.Cluster");
      _kafkaPassword = settings.getProperty("Handlers.Kafka.Password");
      _kafkaUsername = settings.getProperty("Handlers.Kafka.Username");
      _keySerializer = settings.getProperty("Handlers.Kafka.KeySerializer");
      _valueSerializer = settings.getProperty("Handlers.Kafka.ValueSerializer");

      if (_topic == null || _topic.isEmpty())
        _topic = settings.getProperty("Handlers.Kafka.Topic");

      if (_serializer == null)
        _serializer = ConfigureSerializer(settings);
    }
    catch (Exception e)
    {
      _logger.error(e.getMessage(), e);
      System.exit(0);
    }
    finally
    {
      if (propertiesStream != null)
      {
        try
        {
          propertiesStream.close();
        }
        catch (Exception e)
        {
          _logger.error(e.getMessage(), e);
        }
      }
    }
  }

  /****************************************************************************
  * Sets up serialization from configuration settings.
  *
  * @param settings
  *   The properties object with the parsed settings.
  * @return
  *   The serializer
  ****************************************************************************/
  private ISerializer ConfigureSerializer(final Properties settings)
  {
    final String serializationFormat = settings.getProperty("Serialization.Format");

    switch (serializationFormat.toLowerCase())
    {
      case "avro":
        return new AvroSerializer();
      case "json":
        return new JsonSerializer();
      case "text":
        final String fieldDelimiter = settings.getProperty("Serialization.Delimiter");
        return new TextSerializer(fieldDelimiter);
      case "xml":
        return new XmlSerializer();
      default:
        throw new IllegalArgumentException("Unsupported serialization format: " + serializationFormat);
    }
  }

  /****************************************************************************
  * Discovers the Zookeeper and Kafka Broker nodes via the Ambari REST API.
  ****************************************************************************/
  private void DiscoverZookeeperAndBrokers()
  {
    AmbariRestClient ambariRestClient = new AmbariRestClient(_ambariUrl,
      _kafkaCluster,
      _kafkaUsername,
      _kafkaPassword);

    _zookeeperNodes = ambariRestClient.getZookeeperNodes();
    _kafkaBrokerNodes = ambariRestClient.getKafkaNodes();

    for (String node : _zookeeperNodes)
      _zookeeperHost += node + ",";

    _zookeeperHost = _zookeeperHost.substring(0, _zookeeperHost.length() - 2);

    for (String broker : _kafkaBrokerNodes)
      _brokerList += broker + ",";

    _zookeeperHost = _zookeeperHost.substring(0, _zookeeperHost.length() - 2);
  }

  /****************************************************************************
  * Initializes settings for Kafka.
  ****************************************************************************/
  private void InitializeKafka()
  {
    _kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerList);
    _kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, _keySerializer);
    _kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, _valueSerializer);
    _kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "1");
  }
}
