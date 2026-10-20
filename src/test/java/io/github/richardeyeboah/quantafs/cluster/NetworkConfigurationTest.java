package io.github.richardeyeboah.quantafs.cluster;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class NetworkConfigurationTest {

    @Test
    void loopbackClientSeedsRemainTheSafeDefault() {
        assertEquals(
                List.of(
                        NetworkEndpoint.loopback(5101),
                        NetworkEndpoint.loopback(5102),
                        NetworkEndpoint.loopback(5103)),
                RetryCli.parseSeeds(
                        "127.0.0.1:5101,127.0.0.1:5102,127.0.0.1:5103",
                        ClusterSettings.NetworkMode.LOOPBACK));

        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                RetryCli.parseSeeds(
                                        "n1:7000,n2:7000,n3:7000",
                                        ClusterSettings.NetworkMode.LOOPBACK));
        assertTrue(error.getMessage().contains("container network mode"));
    }

    @Test
    void composeClientSeedsRequireExplicitDnsSafeNames() {
        assertEquals(
                List.of(
                        new NetworkEndpoint("n1", 7000),
                        new NetworkEndpoint("n2", 7000),
                        new NetworkEndpoint("n3", 7000)),
                RetryCli.parseSeeds(
                        "n1:7000,n2:7000,n3:7000",
                        ClusterSettings.NetworkMode.COMPOSE));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        RetryCli.parseSeeds(
                                "N1:7000,n2:7000,n3:7000",
                                ClusterSettings.NetworkMode.COMPOSE));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        RetryCli.parseSeeds(
                                "n1:7000,n1:7000,n3:7000",
                                ClusterSettings.NetworkMode.COMPOSE));
    }

    @Test
    void kubernetesClientSeedsRequireTheDedicatedHeadlessService() {
        assertEquals(
                List.of(
                        new NetworkEndpoint("quantafs-0.quantafs-peer", 7000),
                        new NetworkEndpoint("quantafs-1.quantafs-peer", 7000),
                        new NetworkEndpoint("quantafs-2.quantafs-peer", 7000)),
                RetryCli.parseSeeds(
                        "quantafs-0.quantafs-peer:7000,"
                                + "quantafs-1.quantafs-peer:7000,"
                                + "quantafs-2.quantafs-peer:7000",
                        ClusterSettings.NetworkMode.KUBERNETES));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        RetryCli.parseSeeds(
                                "quantafs-0.other-service:7000,"
                                        + "quantafs-1.other-service:7000,"
                                        + "quantafs-2.other-service:7000",
                                ClusterSettings.NetworkMode.KUBERNETES));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        RetryCli.parseSeeds(
                                "QUANTAFS-0.quantafs-peer:7000,"
                                        + "quantafs-1.quantafs-peer:7000,"
                                        + "quantafs-2.quantafs-peer:7000",
                                ClusterSettings.NetworkMode.KUBERNETES));
    }
}
