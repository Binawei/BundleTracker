package bundle.exceptions;

public abstract class BaseException extends Exception {
    protected BaseException(String message) {
        super(message);
    }

    protected BaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
