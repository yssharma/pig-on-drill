package org.apache.drill.distribution.whirr;

import com.google.common.base.Objects;
import org.apache.commons.cli.*;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

import static org.apache.whirr.ClusterSpec.Property.*;

public class EC2Deployment {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EC2Deployment.class);
  static final String drillPropertyFile = "whirr-drill-ec2.properties";

  public enum DeploymentOptions {
    LAUNCH {
      @Override
      void perform(ClusterController controller, ClusterSpec spec) throws IOException, InterruptedException {
        controller.launchCluster(spec);
      }
    },
    DESTROY {
      @Override
      void perform(ClusterController controller, ClusterSpec spec) throws IOException, InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
      }
    };

    abstract void perform(ClusterController controller, ClusterSpec spec) throws IOException, InterruptedException;
  }

  private ClusterSpec createClusterSpec(File whirrConfiuration) throws ConfigurationException {
    CompositeConfiguration compositeConfiguration = new CompositeConfiguration();
    PropertiesConfiguration configuration = new PropertiesConfiguration(whirrConfiuration);
    compositeConfiguration.addConfiguration(configuration);

    ClusterSpec hadoopClusterSpec = new ClusterSpec(compositeConfiguration);

    for (ClusterSpec.Property required : EnumSet.of(CLUSTER_NAME, PROVIDER, IDENTITY, CREDENTIAL,
        INSTANCE_TEMPLATES, PRIVATE_KEY_FILE)) {
      if (hadoopClusterSpec.getConfiguration().getString(required.getConfigName()) == null) {
        throw new IllegalArgumentException(String.format("Option '%s' not set.",
            required.getSimpleName()));
      }
    }

    return hadoopClusterSpec;
  }

  public static int main(String args[]) throws ConfigurationException, ParseException {
    if (!System.getenv().containsKey("AWS_ACCESS_KEY_ID")) {
      logger.error("AWS_ACCESS_KEY_ID is undefined in the current environment");
      return -2;
    }
    if (!System.getenv().containsKey("AWS_SECRET_ACCESS_KEY")) {
      logger.error("AWS_SECRET_ACCESS_KEY is undefined in the current environment");
      return -3;
    }

    CommandLine commandLine = parseArguments(args);

    EC2Deployment deployer = new EC2Deployment();

    String propertyFile = commandLine.getOptionValue("p", drillPropertyFile);
    ClusterSpec spec = deployer.createClusterSpec(new File(propertyFile));

    DeploymentOptions deploymentOption = DeploymentOptions.valueOf(commandLine.getOptionValue("cc").toUpperCase());

    ClusterControllerFactory factory = new ClusterControllerFactory();
    ClusterController controller = factory.create(spec.getServiceName());
    try {
      deploymentOption.perform(controller, spec);
    } catch (IOException | InterruptedException e) {
      logger.error("Error encountered launching cluster: " + e);
      return -1;
    }
    return 0;
  }

  private static CommandLine parseArguments(String[] args) throws ParseException {
    Options options = new Options();
    Option cc = new Option("cc", "cluster-command", true, "Cluster command (launch, destroy)");
    cc.setRequired(true);
    options.addOption(cc);
    Option propertyFile = new Option("p", "property-file", true, "Property file");
    propertyFile.setRequired(false);
    options.addOption(propertyFile);
    BasicParser parser = new BasicParser();
    return parser.parse(options, args);
  }
}
