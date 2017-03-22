package dynamo.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UnsupportedEncodingException;
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
public class XmlSerializer<T> implements ISerializer<T>
{
  private final ObjectMapper _xmlMapper;

  /****************************************************************************
  * Creates an instance of XmlSerializer.
  ****************************************************************************/
  public XmlSerializer()
  {
    _xmlMapper = new ObjectMapper();
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
    return _xmlMapper.reader().readValue(data);
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
    try
    {
      return _xmlMapper.writeValueAsString(data).getBytes("UTF-8");
    }
    catch (UnsupportedEncodingException uee)
    {
      // Om nom nom, this error should never happen
    }
    catch (JsonProcessingException jpe)
    {
      jpe.printStackTrace();
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
      final byte[] header = "<xml version=\"1.0\">".getBytes();
      byte[] combined = new byte[serializedData.length + header.length];

      System.arraycopy(header, 0, combined, 0, header.length);
      System.arraycopy(serializedData, 0, combined, header.length, serializedData.length);
      serializedData = combined;

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