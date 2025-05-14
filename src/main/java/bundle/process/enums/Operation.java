package bundle.process.enums;

public enum Operation {
    FILTER("filter"),
    PROCESS("process");

    private final String code;

    Operation(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
