package edu.utexas.tacc.tapis.jobs.model;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Strings;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.utexas.tacc.tapis.shared.uri.TapisUrl.TAPIS_PROTOCOL_PREFIX;

/*
 * Class representing a source or destination in a Tapis transfer URI element.
 * Each transfer request can contain multiple elements.
 *
 * A Transfer URI must look like tapis://{systemId}/{path}
 *                            or https://{systemId}/{path}
 *                            or  http://{systemId}/{path}
 * For protocol http/s:// the systemId is the IP address or host name
 * For protocol tapis:// systemId is the Tapis system id.
 */
public class TransferURI
{
  private final String systemId;
  private final String path;
  private final String protocol;
  private final boolean isTapisProtocol;
  private static final Pattern pattern = Pattern.compile("(http:\\/\\/|https:\\/\\/|tapis:\\/\\/)([\\w -\\.]+)\\/?(.*)");

  // Construct using a single string for the URI
  public TransferURI(String stringURI) throws TapisException
  {
    Matcher matcher = pattern.matcher(stringURI);
    // If uri does not match the pattern it is an error
    if (!matcher.find()) { throw new TapisException("Invalid transfer URI: " + stringURI); }
    // Extract the systemId, for http/s this is the ip address or host name
    systemId = matcher.group(2);
    // Extract and normalize the path. If no path set then use /
    var tmp = Optional.ofNullable(matcher.group(3)).orElse("/");
    path = FilenameUtils.normalize(tmp);
    // Extract the path and protocol
    protocol = matcher.group(1);
    isTapisProtocol = stringURI.startsWith(TAPIS_PROTOCOL_PREFIX);
  }

  // Construct given specific values for protocol, systemId and path
  public TransferURI(TransferURI uri1, String path1)
  {
    path = FilenameUtils.normalize(path1);
    systemId = uri1.getSystemId();
    protocol = uri1.getProtocol();
    isTapisProtocol = uri1.isTapisProtocol;
  }

  public String getProtocol() { return protocol; }
  public String getSystemId() { return systemId; }
  public String getPath() { return path; }
  public boolean isTapisProtocol() { return isTapisProtocol; }

  public String toString()
  {
    String tmpPath = Strings.CS.removeStart(path, "/");
    return String.format("%s%s/%s", protocol, systemId, tmpPath);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransferURI that = (TransferURI) o;
    return Objects.equals(systemId, that.systemId) && Objects.equals(path, that.path) && Objects.equals(protocol, that.protocol);
  }

  @Override
  public int hashCode() {
    return Objects.hash(systemId, path, protocol);
  }
}
