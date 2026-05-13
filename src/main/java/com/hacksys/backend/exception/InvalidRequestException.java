package com.hacksys.backend.exception;

/**
 * Thrown when a service method receives a request that is missing required context
 * (e.g. a null user ID on order submission). Signals a 400-class caller error so
 * the pipeline is never entered and no partial state is created.
 */
public class InvalidRequestException extends RuntimeException {

    public InvalidRequestException(String message) {
        super(message);
    }

    public InvalidRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
