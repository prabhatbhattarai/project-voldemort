package voldemort.protocol.pb;

import voldemort.protocol.AbstractWireFormatTest;
import voldemort.protocol.WireFormatType;

public class ProtocolBuffersWireFormatTest extends AbstractWireFormatTest {

    public ProtocolBuffersWireFormatTest() {
        super(WireFormatType.PROTOCOL_BUFFERS);
    }

}
