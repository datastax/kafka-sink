package com.datastax.kafkaconnector;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.kafkaconnector.cloud.SNIProxyServer;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import java.util.List;

public class ConnectorSettingsProvider {
  private int binaryPort;
  private List<EndPoint> contactPoints;
  private String localDc;
  private boolean hasDateRange;
  private boolean isDse;

  public ConnectorSettingsProvider(
      int binaryPort,
      List<EndPoint> contactPoints,
      String localDc,
      boolean hasDateRange,
      boolean isDse) {
    this.binaryPort = binaryPort;
    this.contactPoints = contactPoints;
    this.localDc = localDc;
    this.hasDateRange = hasDateRange;
    this.isDse = isDse;
  }

  public static ConnectorSettingsProvider newInstance(CCMCluster ccm) {
    return new ConnectorSettingsProvider(
        ccm.getBinaryPort(),
        ccm.getInitialContactPoints(),
        ccm.getDC(1),
        // DSE 5.0 doesn't have the DateRange type; neither does DDAC, so we need to account for
        // that
        // in our testing.
        ccm.getClusterType() == DSE
            && Version.isWithinRange(Version.parse("5.1.0"), null, ccm.getVersion()),
        ccm.getClusterType() == DSE);
  }

  public static ConnectorSettingsProvider newInstance(SNIProxyServer proxy) {
    return new ConnectorSettingsProvider(
        proxy.getPort(),
        proxy.getContactPoints(),
        proxy.getLocalDCName(),
        true, // sni proxy runs always for DSE >= 6.8.0
        true);
  }

  public int getBinaryPort() {
    return binaryPort;
  }

  public void setBinaryPort(int binaryPort) {
    this.binaryPort = binaryPort;
  }

  public List<EndPoint> getContactPoints() {
    return contactPoints;
  }

  public void setContactPoints(List<EndPoint> contactPoints) {
    this.contactPoints = contactPoints;
  }

  public String getLocalDc() {
    return localDc;
  }

  public void setLocalDc(String localDc) {
    this.localDc = localDc;
  }

  public boolean hasDateRange() {
    return hasDateRange;
  }

  public void setHasDateRange(boolean hasDateRange) {
    this.hasDateRange = hasDateRange;
  }

  public boolean isDse() {
    return isDse;
  }

  public void setDse(boolean dse) {
    isDse = dse;
  }
}
