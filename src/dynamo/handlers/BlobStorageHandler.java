package dynamo.handlers;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import dynamo.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

/******************************************************************************
* Appends data to an AppendBlob in Azure Blob Storage.
******************************************************************************/
public class BlobStorageHandler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(BlobStorageHandler.class);

  private CloudBlobClient _blobClient;
  private CloudBlobContainer _blobContainer;
  private CloudBlobDirectory _outputDirectory;
  private String _outputFilename;
  private ISerializer _serializer;
  private CloudStorageAccount _storageAccount;

  /****************************************************************************
  * Creates an instance of BlobStorageHandler.
  ****************************************************************************/
  public BlobStorageHandler()
  {
    CheckConfiguration();
  }

  /****************************************************************************
  * Creates an instance of BlobStorageHandler.
  *
  * @param serializer
  *   The serializer.
  ****************************************************************************/
  public BlobStorageHandler(ISerializer serializer)
  {
    _serializer = serializer;
    CheckConfiguration();
  }

  /****************************************************************************
  * Appends the data to the AppendBlob.
  *
  * @param data
  *   The data.
  * @throws Throwable
  *   Any uncaught exception.
  ****************************************************************************/
  public void HandleData(Object data) throws Throwable
  {
    final CloudAppendBlob appendBlob;
    final byte[] serializedData;

    appendBlob = _outputDirectory.getAppendBlobReference(_outputFilename);

    if (!appendBlob.exists())
      appendBlob.createOrReplace();

    if (_serializer == null)
      throw new NullPointerException("No serialization set.");

    serializedData = _serializer.Serialize(data);

    if (serializedData != null && serializedData.length > 0)
      appendBlob.appendFromByteArray(serializedData, 0, serializedData.length);
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

      final String connectionString = settings.getProperty("Handlers.BlobStorage.ConnectionString");
      final String containerName = settings.getProperty("Handlers.BlobStorage.ContainerName");
      final String outputDirectoryName = settings.getProperty("Handlers.BlobStorage.OutputDirectory");

      _storageAccount = CloudStorageAccount.parse(connectionString);
      _blobClient = _storageAccount.createCloudBlobClient();
      _blobContainer = _blobClient.getContainerReference(containerName);
      _outputDirectory = _blobContainer.getDirectoryReference(outputDirectoryName);
      _outputFilename = settings.getProperty("Handlers.BlobStorage.OutputFilename");

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
}
