package dynamo.serialization;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Future;

/******************************************************************************
* Serializes an object to XML using the Jackson library.
******************************************************************************/
public class TextSerializer<T> implements ISerializer<T>
{
  private final String _fieldDelimiter;

  /****************************************************************************
  * Creates an instance of TextSerializer.
  ****************************************************************************/
  public TextSerializer()
  {
    _fieldDelimiter = "\t";
  }

  /****************************************************************************
  * Creates an instance of TextSerializer.
  *
  * @param fieldDelimiter
  *   The field delimiter.
  ****************************************************************************/
  public TextSerializer(final String fieldDelimiter)
  {
    _fieldDelimiter = fieldDelimiter;
  }

  /****************************************************************************
  * Serializes an object.
  *
  * @param data
  *   The serialized data.
  * @param typeClass
  *   The generic type's class.
  * @return
  *   The object.
  * @throws Throwable
  *   Any exception that occurs while deserializing.
  ****************************************************************************/
  public T Deserialize(final String data, final Class<T> typeClass) throws Exception
  {
    final T generic = typeClass.newInstance();
    final Field[] properties = typeClass.getDeclaredFields();
    final String[] fields = data.split(_fieldDelimiter);

    for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++)
      properties[fieldIndex].set(generic, fields[fieldIndex]);

    return generic;
  }

  /****************************************************************************
  * Serializes an object.
  *
  * @param data
  *   The object.
  * @return
  *   The serialized data in a byte array.
  * @throws Throwable
  *   Any exception that occurs while serializing.
  ****************************************************************************/
  public byte[] Serialize(final T data) throws Exception
  {
    Class<?> dataClass = data.getClass();
    StringBuilder serializedData = new StringBuilder();

    try
    {
      for (Field property : dataClass.getDeclaredFields())
      {
        if (Modifier.isPublic(property.getModifiers()))
        {
          serializedData.append(property.get(data).toString());
          serializedData.append(_fieldDelimiter);
        }
      }

      for (Method method : dataClass.getMethods())
      {
        if (method.getName().startsWith("get") && method.getName() != "getClass")
        {
          serializedData.append(method.invoke(data));
          serializedData.append(_fieldDelimiter);
        }
      }

      return serializedData.toString().getBytes("UTF-8");
    }
    catch (IllegalAccessException|UnsupportedEncodingException e)
    {
      // Swallow it, this should never happen
    }

    return null;
  }

  /****************************************************************************
  * Serializes an object to file. If the file exists, it will be appended to.
  *
  * @param data
  *   The object.
  * @param filename
  *   The full path to the output file.
  * @throws Throwable
  *   Any exception that occurs while serializing.
  ****************************************************************************/
  public void Serialize(final T data, final String filename) throws Exception
  {
    final Path outputPath = Paths.get(filename);
    byte[] serializedData = Serialize(data);

    if (outputPath.toFile().exists())
    {
      final byte[] recordDelimiter = "\r\n".getBytes();
      byte[] combined = new byte[serializedData.length + recordDelimiter.length];

      System.arraycopy(recordDelimiter, 0, combined, 0, recordDelimiter.length);
      System.arraycopy(serializedData, 0, combined, recordDelimiter.length, serializedData.length);
      serializedData = combined;
    }
    else
    {
      Files.createDirectories(outputPath.getParent());
      Files.createFile(outputPath);
    }

    if (serializedData != null && serializedData.length > 0)
    {
      final AsynchronousFileChannel asyncFile = AsynchronousFileChannel.open(outputPath, StandardOpenOption.WRITE);
      final ByteBuffer buffer = ByteBuffer.wrap(serializedData);
      final long position = asyncFile.size();
      final Future<Integer> writeOperation = asyncFile.write(buffer, position);

      while (!writeOperation.isDone());

      asyncFile.close();
    }
  }
}