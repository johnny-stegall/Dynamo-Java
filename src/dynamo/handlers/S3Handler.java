package dynamo.handlers;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import dynamo.serialization.ISerializer;
import dynamo.serialization.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by John on 3/13/2017.
 */
public class S3Handler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(EventHubsHandler.class);
  private final ISerializer _serializer;

  private String _bucket;
  private AmazonS3 _client;
  private String _secretKey;

  /****************************************************************************
  * Creates an instance of KinesisHandler.
  ****************************************************************************/
  public S3Handler()
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

    if (!_client.doesBucketExist(_bucket))
      _client.createBucket(_bucket);

    _client.putObject(_bucket, _secretKey, serializedData.toString());
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

      final String accessKey = settings.getProperty("Handlers.S3.AccessKey");
      _secretKey = settings.getProperty("Handlers.S3.SecretKey");

      final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, _secretKey);
      final AWSStaticCredentialsProvider provider = new AWSStaticCredentialsProvider(credentials);

      _bucket = settings.getProperty("Handlers.S3.Bucket");
      _client = AmazonS3Client.builder()
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
