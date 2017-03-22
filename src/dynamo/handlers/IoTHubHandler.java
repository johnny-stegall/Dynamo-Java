package dynamo.handlers;

import com.microsoft.azure.iot.service.sdk.Device;
import com.microsoft.azure.iot.service.sdk.RegistryManager;
import com.microsoft.azure.iothub.DeviceClient;
import com.microsoft.azure.iothub.IotHubClientProtocol;
import com.microsoft.azure.iothub.Message;
import dynamo.iot.IoTEventCallback;
import dynamo.iot.MessageReceivedCallback;
import dynamo.serialization.ISerializer;
import dynamo.serialization.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

/******************************************************************************
* Sends data to Azure IoT Hub as messages.
******************************************************************************/
public class IoTHubHandler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(IoTHubHandler.class);
  private final ISerializer _serializer;

  private Device _device;
  private DeviceClient _client;

  /****************************************************************************
  * Creates an instance of IoTHubHandler.
  ****************************************************************************/
  public IoTHubHandler()
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
    final Message message = new Message(serializedData);
    message.setExpiryTime(5000);

    Object lock = new Object();
    final IoTEventCallback iotCallback = new IoTEventCallback();
    _client.sendEventAsync(message, iotCallback, lock);
    _logger.info("Sending message to IoT Hub...");

    synchronized (lock)
    {
      lock.wait();
    }
  }

  /****************************************************************************
  * Destructors aren't reliable in Java, but it's good to call this here
  * anyway.
  ****************************************************************************/
  protected void finalize()
  {
    try
    {
      _client.close();
    }
    catch (Exception e)
    {
      _logger.error(e.getMessage(), e);
    }
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

      final String registryConnectionString = settings.getProperty("Handlers.IoTHub.ConnectionString.Registry");
      final RegistryManager registryManager = RegistryManager.createFromConnectionString(registryConnectionString);
      final String deviceId = settings.getProperty("Handlers.IoTHub.DeviceId", UUID.randomUUID().toString());
      final boolean createDevice = Boolean.parseBoolean(settings.getProperty("Handlers.IoTHub.CreateDevice"));

      if (createDevice)
        CreateDevice(settings, registryManager, deviceId);
      else
        _device = registryManager.getDevice(deviceId);

      final IotHubClientProtocol protocol = IotHubClientProtocol.valueOf(settings.getProperty("Handlers.IoTHub.Protocol", "AMQPS"));
      final String deviceConnectionString = settings.getProperty("Handlers.IoTHub.ConnectionString.Device");
      final MessageReceivedCallback messageCallback = new MessageReceivedCallback();

      _client = new DeviceClient(deviceConnectionString, protocol);
      _client.setMessageCallback(messageCallback, null);
      _client.open();
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
  * Registers a new device with IoT Hub and then updates the properties file
  * with the new device connection string.
  *
  * @param settings
  *   The settings.
  * @param registryManager
  *   The IoT Hub registry manager.
  * @param deviceId
  *   The device ID.
  ****************************************************************************/
  private void CreateDevice(final Properties settings, final RegistryManager registryManager, final String deviceId) throws Exception
  {
    _device = Device.createFromId(deviceId, null, null);
    _device = registryManager.addDevice(_device);

    final String registryConnectionString = settings.getProperty("Handlers.IoTHub.ConnectionString.Registry");
    final String hostName = registryConnectionString.substring(registryConnectionString.indexOf("HostName"),
      registryConnectionString.indexOf(";") - 1);
    final String primaryKey = _device.getPrimaryKey();

    final String deviceConnectionString = hostName
      + "DeviceId=" + deviceId
      + "SharedAccessKey=" + primaryKey;

    settings.setProperty("Handlers.IoTHub.ConnectionString.Device", deviceConnectionString);
  }
}