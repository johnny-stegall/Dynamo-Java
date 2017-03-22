package dynamo.serialization;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/******************************************************************************
* Serializes an object to AVRO.
******************************************************************************/
public class AvroSerializer<T> implements ISerializer<T>
{
  private Schema _schema;

  /****************************************************************************
  * Creates an instance of AvroSerializer.
  ****************************************************************************/
  public AvroSerializer()
  {
    CheckConfiguration();
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
    final DatumReader<T> deserializer = new SpecificDatumReader<>();
    final InputStream inputStream = new ByteArrayInputStream(data.getBytes());
    final Decoder avroDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);

    //T generic = typeClass.newInstance();
    return deserializer.read(null, avroDecoder);
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
    if (_schema == null)
    {
      Class<?> dataClass = data.getClass();
      _schema = new ReflectData.AllowNull().getSchema(dataClass);
    }

    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    final Encoder encoder = EncoderFactory.get().binaryEncoder(buffer, null);
    final DatumWriter serializer = new ReflectDatumWriter(_schema);

    serializer.write(data, encoder);
    encoder.flush();

    return buffer.toByteArray();
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
    if (_schema == null)
    {
      Class<?> dataClass = data.getClass();
      _schema = new ReflectData.AllowNull().getSchema(dataClass);
    }

    final Path outputPath = Paths.get(filename);

    try
    {
      final DatumWriter serializer = new ReflectDatumWriter(_schema);
      final DataFileWriter dataFileWriter = new DataFileWriter(serializer);
      dataFileWriter.setCodec(CodecFactory.snappyCodec());

      if (outputPath.toFile().exists())
        dataFileWriter.appendTo(outputPath.toFile());
      else
      {
        Files.createDirectories(outputPath.getParent());
        dataFileWriter.create(_schema, outputPath.toFile());
      }

      dataFileWriter.append(data);
      dataFileWriter.close();
    }
    catch (Exception e)
    {
      try
      {
        // Sometimes reflection fails, so try with generic records
        final DatumWriter<GenericRecord> serializer = new ReflectDatumWriter<GenericRecord>(_schema);
        final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(serializer);

        if (!outputPath.toFile().exists())
          dataFileWriter.create(_schema, outputPath.toFile());
        else
          dataFileWriter.appendTo(outputPath.toFile());

        dataFileWriter.append(CreateGenericRecord(data));
        dataFileWriter.close();
      }
      catch (Exception ie)
      {
        ie.printStackTrace();
        e.printStackTrace();
      }
    }
  }

  /****************************************************************************
  * Checks for a properties file to load settings.
  ****************************************************************************/
  private void CheckConfiguration()
  {
    try
    {
      final String propertiesFile = Paths.get("dynamo.properties").toAbsolutePath().toString();
      final InputStream propertiesStream = new FileInputStream(propertiesFile);
      final Properties settings = new Properties();
      settings.load(propertiesStream);

      String schemaFile = settings.getProperty("Serialization.SchemaFile");

      if (schemaFile != null && !schemaFile.isEmpty())
      {
        schemaFile = Paths.get(schemaFile).toAbsolutePath().toString();
        _schema = new Schema.Parser().parse(new File(schemaFile));
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }
  }

  /****************************************************************************
  * Creates a generic data record using the AVRO schema for the data.
  *
  * @param data
  *   The object.
  * @return
  *   The serialized record.
  * @throws IllegalAccessException
  *   When a field cannot be accessed.
  * @throws InvocationTargetException
  *   When a get method can't be invoked.
  ****************************************************************************/
  private GenericRecord CreateGenericRecord(final T data) throws IllegalAccessException, InvocationTargetException
  {
    GenericRecord record = new GenericData.Record(_schema);
    Class<?> dataClass = data.getClass();

    for (Field property : dataClass.getDeclaredFields())
    {
      if (Modifier.isPublic(property.getModifiers()))
        record.put(property.getName(), property.get(data));
    }

    for (Method method : dataClass.getMethods())
    {
      if (method.getName().startsWith("get") && method.getName() != "getClass")
      {
        String fieldName = method.getName().replace("get", "");
        char fieldCharacters[] = fieldName.toCharArray();
        fieldCharacters[0] = Character.toLowerCase(fieldCharacters[0]);
        fieldName = new String(fieldCharacters);
        record.put(fieldName, method.invoke(data));
      }
    }

    return record;
  }
}