package voldemort.protocol;

public enum WireFormatType {
    VOLDEMORT("vold"),
    PROTOCOL_BUFFERS("pb");

    private final String name;

    private WireFormatType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static WireFormatType fromName(String name) {
        for(WireFormatType type: WireFormatType.values())
            if(type.getName().equals(name))
                return type;
        throw new IllegalArgumentException("No wire format '" + name + "' was found");
    }

}
