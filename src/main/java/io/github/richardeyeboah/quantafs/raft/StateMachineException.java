package io.github.richardeyeboah.quantafs.raft;

/** A committed entry could not be applied, or the state machine is unusable. */
public class StateMachineException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StateMachineException(String message) {
        super(message);
    }

    public StateMachineException(String message, Throwable cause) {
        super(message, cause);
    }
}
