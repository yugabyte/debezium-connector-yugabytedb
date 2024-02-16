package io.debezium.connector.yugabytedb.connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to denote YugabyteDB version.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBVersion implements Comparable<YBVersion> {
  public static final String DEFAULT_YB_VERSION = "2.21.0.0";
  public Integer major;
  public Integer minor;
  public Integer patch = 0;
  public Integer revision = 0;

  public YBVersion(int major, int minor, int patch, int revision) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.revision = revision;
  }

  public YBVersion(String versionString) {
    assert !versionString.isEmpty();
    String[] version = versionString.split("\\.");

    this.major = Integer.parseInt(version[0]);
    this.minor = Integer.parseInt(version[1]);

    if (version.length >= 3) {
      this.patch = Integer.parseInt(version[2]);
    }

    if (version.length == 4) {
      this.revision = Integer.parseInt(version[3]);
    }
  }

  @Override
  public int compareTo(YBVersion o) {
    if (!this.major.equals(o.major)) {
      return this.major.compareTo(o.major);
    } else if (!this.minor.equals(o.minor)) {
      return this.minor.compareTo(o.minor);
    } else if (!this.patch.equals(o.patch)) {
      return this.patch.compareTo(o.patch);
    } else if (!this.revision.equals(o.revision)) {
      return this.revision.compareTo(o.revision);
    }

    // Both the versions are equal.
    return 0;
  }

  @Override
  public String toString() {
    return String.format("%s.%s.%s.%s", major, minor, patch, revision);
  }

  public static YBVersion getCurrentYBVersionEnv() {
    String imageName = System.getenv("YB_DOCKER_IMAGE");

    // If no environment variable is specified, it will be assumed that the current YugabyteDB
    // version is the default version i.e. latest. This needs to be updated every time YugabyteDB's
    // latest version changes.
    if (imageName.isEmpty()) {
      return new YBVersion(DEFAULT_YB_VERSION);
    }

    String regexPattern = "yugabyte:(.*?)-b*";
    Pattern pattern = Pattern.compile(regexPattern);
    Matcher matcher = pattern.matcher(imageName);

    if (matcher.find()) {
      return new YBVersion(matcher.group(1));
    }

    return new YBVersion(DEFAULT_YB_VERSION);
  }

  public static YBVersion getCurrentYBVersion(Connection conn) {
    try (Statement st = conn.createStatement()) {
      ResultSet rs = st.executeQuery("select version();");

      if (rs.next()) {
        String fullVersionString = rs.getString("version");

        String regexPattern = "YB-(.*?)-b*";
        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher = pattern.matcher(fullVersionString);

        if (matcher.find()) {
          return new YBVersion(matcher.group(1));
        }
      }

      return new YBVersion(DEFAULT_YB_VERSION);
    } catch (SQLException sqle) {
      throw new RuntimeException("Exception while trying to get current YB version", sqle);
    }
  }
}