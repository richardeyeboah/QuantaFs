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
        assertTrue(error.getMessage().contains("--network-mode compose"));
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
}
