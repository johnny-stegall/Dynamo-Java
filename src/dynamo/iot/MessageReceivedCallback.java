package dynamo.iot;

import com.microsoft.azure.iothub.IotHubMessageResult;
import com.microsoft.azure.iothub.Message;
import com.microsoft.azure.iothub.MessageProperty;

/****************************************************************************
* Executed when IoT Hub receives a message and a cloud service should respond
* to it.
****************************************************************************/
public class MessageReceivedCallback implements com.microsoft.azure.iothub.MessageCallback
{
  public IotHubMessageResult execute(Message message, Object context)
  {
    System.out.println("Message received:");
    System.out.println();
    System.out.println("Correlation ID: " + message.getCorrelationId());
    System.out.println("Message ID: " + message.getMessageId());

    for (MessageProperty property : message.getProperties())
      System.out.println(property.getName() + ": " + property.getValue());

    System.out.println("Content: " + new String(message.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET));
    System.out.println();

    IotHubMessageResult result = IotHubMessageResult.COMPLETE;
    System.out.println("Responding to message: " + message.getMessageId() + " with " + result.name());
    return result;
  }
}