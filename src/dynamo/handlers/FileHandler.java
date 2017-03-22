package dynamo.handlers;

import dynamo.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/******************************************************************************
* Writes data to a file.
******************************************************************************/
public class FileHandler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(FileHandler.class);

  private String _path;
  private String _filename;
  private ISerializer _serializer;

  /****************************************************************************
  * Creates an instance of FileHandler.
  ****************************************************************************/
  public FileHandler()
  {
    this(null, null);
  }

  /****************************************************************************
  * Creates an instance of FileHandler.
  *
  * @param serializer
  *   The serializer.
  ****************************************************************************/
  public FileHandler(ISerializer serializer)
  {
    this(serializer, null, null);
  }

  /****************************************************************************
  * Creates an instance of FileHandler.
  *
  * @param filename
  *   The filename.
  ****************************************************************************/
  public FileHandler(String filename)
  {
    this(null, filename, null);
  }

  /****************************************************************************
  * Creates an instance of FileHandler.
  *
  * @param filename
  *   The filename.
  * @param path
  *   The full path to the file.
  ****************************************************************************/
  public FileHandler(String filename, String path)
  {
    this(null, filename, path);
  }

  /****************************************************************************
  * Creates an instance of FileHandler.
  *
  * @param serializer
  *   The serializer.
  * @param filename
  *   The filename.
  * @param path
  *   The full path to the file.
  ****************************************************************************/
  public FileHandler(ISerializer serializer, String filename, String path)
  {
    _serializer = serializer;
    _filename = filename;
    _path = path;
    CheckConfiguration();
  }

  /****************************************************************************
  * Handles generated data.
  *
  * @param data
  *   The data.
  * @throws Throwable
  *   Any uncaught exception.
  ****************************************************************************/
  @Override
  public void HandleData(final Object data) throws Throwable
  {
    final Path outputPath = Paths.get(_path + _filename);
    _serializer.Serialize(data, outputPath.toString());
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

      if (_path == null || _path.isEmpty())
        _path = settings.getProperty("Handlers.File.Path");

      if (!_path.endsWith("\\"))
        _path += "\\";

      if (_serializer == null)
        _serializer = ConfigureSerializer(settings);

      if (_filename == null || _filename.isEmpty())
        _filename = settings.getProperty("Handlers.File.Filename");

      if (_filename.lastIndexOf('.') == -1)
        _filename += "." + GetSerializationExtension(settings);
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
  *   The serializer.
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
        final String fieldDelimiter = settings.getProperty("Serialization.Delimiter", "\t");
        return new TextSerializer(fieldDelimiter);
      case "xml":
        return new XmlSerializer();
      default:
        throw new IllegalArgumentException("Unsupported serialization format: " + serializationFormat);
    }
  }

  /****************************************************************************
  * Gets the file extension for the serialization format.
  *
  * @param settings
  *   The properties object with the parsed settings.
  * @return
  *   The filename extension.
  ****************************************************************************/
  private String GetSerializationExtension(final Properties settings)
  {
    final String serializationFormat = settings.getProperty("Serialization.Format");

    switch (serializationFormat.toLowerCase())
    {
      case "avro":
      case "json":
      case "xml":
        return serializationFormat.toLowerCase();
      case "text":
        final String fieldDelimiter = settings.getProperty("Serialization.Delimiter", "\t");

        if (fieldDelimiter == "\t")
          return "tsv";
        else if (fieldDelimiter == ",")
          return "csv";
        else
          return "txt";
      default:
        throw new IllegalArgumentException("Unsupported serialization format: " + serializationFormat);
    }
  }
}
