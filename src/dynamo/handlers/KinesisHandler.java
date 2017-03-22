package dynamo.handlers;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import dynamo.serialization.ISerializer;
import dynamo.serialization.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by John on 3/13/2017.
 */
public class KinesisHandler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(EventHubsHandler.class);
  private final ISerializer _serializer;

  private AmazonKinesis _client;
  private String _stream;

  /****************************************************************************
  * Creates an instance of KinesisHandler.
  ****************************************************************************/
  public KinesisHandler()
  {
    CheckConfiguration();

    _serializer = new JsonSerializer();
  }

  /****************************************************************************
  * Serializes the data to a byte array and sends it into AWS Kinesis as
  * a message.
  *
  * @param data
  *   The data.
  * @throws Throwable
  *   Any uncaught exception.
  ****************************************************************************/
  public void HandleData(Object data) throws Throwable
  {
    final byte[] serializedData = _serializer.Serialize(data);

    PutRecordRequest putRecordRequest = new PutRecordRequest();
    putRecordRequest.setStreamName(_stream);
    putRecordRequest.setData(ByteBuffer.wrap(serializedData));
    _client.putRecord(putRecordRequest);
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

      final String accessKey = settings.getProperty("Handlers.Kinesis.AccessKey");
      final String secretKey = settings.getProperty("Handlers.Kinesis.SecretKey");

      final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      final AWSStaticCredentialsProvider provider = new AWSStaticCredentialsProvider(credentials);

      _stream = settings.getProperty("Handlers.Kinesis.Stream");
      _client = AmazonKinesisClient.builder()
        .withCredentials(provider)
        .build();
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
}
