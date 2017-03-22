package dynamo.handlers;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;
import dynamo.serialization.ISerializer;
import dynamo.serialization.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/******************************************************************************
* Sends data to Azure Event Hubs as messages.
******************************************************************************/
public class EventHubsHandler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(EventHubsHandler.class);
  private final ISerializer _serializer;

  private EventHubClient _client;

  /****************************************************************************
  * Creates an instance of EventHubsHandler.
  ****************************************************************************/
  public EventHubsHandler() throws IOException, ServiceBusException
  {
    CheckConfiguration();

    _serializer = new JsonSerializer();
  }

  /****************************************************************************
  * Serializes the data to a byte array and sends it into Azure Event Hubs as
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
    final EventData eventData = new EventData(serializedData);
    final CompletableFuture<Void> sendData = _client.send(eventData);
    sendData.get();
  }

  /****************************************************************************
  * Destructors aren't reliable in Java, but it's good to call this here
  * anyway.
  ****************************************************************************/
  protected void finalize()
  {
    _client.close();
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

      final String namespace = settings.getProperty("Handlers.EventHubs.Namespace");
      final String name = settings.getProperty("Handlers.EventHubs.Name");
      final String keyName = settings.getProperty("Handlers.EventHubs.KeyName");
      final String key = settings.getProperty("Handlers.EventHubs.Key");

      final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(namespace,
        name,
        keyName,
        key);

      _client = EventHubClient.createFromConnectionStringSync(connectionString.toString());
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
