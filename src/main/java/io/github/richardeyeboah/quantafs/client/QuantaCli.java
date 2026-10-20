package io.github.richardeyeboah.quantafs.client;

import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.DeleteRequest;
import io.github.richardeyeboah.quantafs.api.v1.GetRequest;
import io.github.richardeyeboah.quantafs.api.v1.GetResponse;
import io.github.richardeyeboah.quantafs.api.v1.KeyValueServiceGrpc;
import io.github.richardeyeboah.quantafs.api.v1.PutRequest;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Minimal demonstration client. Keys and values are taken from the command line as UTF-8 text;
 * the API itself is byte-oriented.
 */
public final class QuantaCli {

    public static final int EXIT_OK = 0;
    public static final int EXIT_NOT_FOUND = 1;
    public static final int EXIT_USAGE = 2;
    public static final int EXIT_RPC_ERROR = 3;

    public static final String DEFAULT_TARGET = "127.0.0.1:50051";

    public static final String USAGE =
            """
            Usage: client [--target <host:port>] [--timeout-seconds <n>] <command>

            Commands:
              put <key> <value>   create or overwrite a key; prints OK
              get <key>           print the value; exit 1 if the key does not exist
              delete <key>        remove a key (succeeds if missing); prints OK

            Options:
              --target            server address (default 127.0.0.1:50051)
              --timeout-seconds   per-request deadline (default 5)

            Exit codes: 0 success, 1 key not found, 2 usage error, 3 RPC error
            """;

    private QuantaCli() {}

    /** Runs one command and returns a process exit code. */
    public static int run(String[] args, PrintStream out, PrintStream err) {
        String target = DEFAULT_TARGET;
        long timeoutSeconds = 5;
        List<String> positional = new ArrayList<>();
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--target" -> target = requireValue(args, ++i, "--target");
                    case "--timeout-seconds" ->
                            timeoutSeconds = Long.parseLong(requireValue(args, ++i, "--timeout-seconds"));
                    case "-h", "--help" -> {
                        out.print(USAGE);
                        return EXIT_OK;
                    }
                    default -> positional.add(args[i]);
                }
            }
            if (timeoutSeconds <= 0) {
                throw new IllegalArgumentException("--timeout-seconds must be positive");
            }
        } catch (IllegalArgumentException e) {
            return usageError(err, e.getMessage());
        }

        if (positional.isEmpty()) {
            return usageError(err, "missing command");
        }
        String command = positional.get(0);
        int expectedArgs =
                switch (command) {
                    case "put" -> 3;
                    case "get", "delete" -> 2;
                    default -> -1;
                };
        if (expectedArgs < 0) {
            return usageError(err, "unknown command: " + command);
        }
        if (positional.size() != expectedArgs) {
            return usageError(err, "wrong number of arguments for " + command);
        }

        ManagedChannel channel =
                Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        try {
            KeyValueServiceGrpc.KeyValueServiceBlockingStub stub =
                    KeyValueServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
            ByteString key = utf8(positional.get(1));
            switch (command) {
                case "put" -> {
                    stub.put(PutRequest.newBuilder().setKey(key).setValue(utf8(positional.get(2))).build());
                    out.println("OK");
                }
                case "get" -> {
                    GetResponse response = stub.get(GetRequest.newBuilder().setKey(key).build());
                    if (!response.getFound()) {
                        err.println("NOT FOUND");
                        return EXIT_NOT_FOUND;
                    }
                    out.println(response.getValue().toStringUtf8());
                }
                case "delete" -> {
                    stub.delete(DeleteRequest.newBuilder().setKey(key).build());
                    out.println("OK");
                }
                default -> throw new IllegalStateException(command);
            }
            return EXIT_OK;
        } catch (StatusRuntimeException e) {
            String description = e.getStatus().getDescription();
            err.println(
                    "error: "
                            + e.getStatus().getCode()
                            + (description == null ? "" : ": " + description));
            return EXIT_RPC_ERROR;
        } finally {
            channel.shutdownNow();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        return args[index];
    }

    private static ByteString utf8(String text) {
        return ByteString.copyFrom(text, StandardCharsets.UTF_8);
    }

    private static int usageError(PrintStream err, String message) {
        err.println("error: " + message);
        err.print(USAGE);
        return EXIT_USAGE;
    }
}
