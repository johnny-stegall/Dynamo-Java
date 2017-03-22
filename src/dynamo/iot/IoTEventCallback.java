package dynamo.iot;

import com.microsoft.azure.iothub.IotHubEventCallback;
import com.microsoft.azure.iothub.IotHubStatusCode;

/****************************************************************************
* Executed after IoT Hub processes a message from this device and responds
* with an acknowledgement.
****************************************************************************/
public class IoTEventCallback implements IotHubEventCallback
{
  public void execute(IotHubStatusCode status, Object context)
  {
    System.out.println("IoT Hub responded to message with status: " + status.name());

    if (context != null)
    {
      synchronized (context)
      {
        context.notify();
      }
    }
  }
}
